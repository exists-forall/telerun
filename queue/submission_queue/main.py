import http.server
import ssl
import os
import time
import json
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs
import traceback

import submission_queue.db as db

max_submit_size = 1 << 20 # 1 MB

max_completed_job_age = 60 * 10 # 10 minutes
max_claimed_job_age = 60 * 10 # 10 minutes

def main():
    con = db.connect_to_db()

    audit_log = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "audit_log.jsonl"), "a")

    def curr_timestamp():
        return datetime.now(timezone.utc).isoformat()

    def authenticate_user(username, token):
        c = con.cursor()
        c.execute('''
        SELECT * FROM users
        WHERE username = ? AND token = ?
        ''', (username, token))
        return c.fetchone() is not None
    
    def authenticate_executor(executor, token):
        c = con.cursor()
        c.execute('''
        SELECT * FROM executors
        WHERE name = ? AND token = ?
        ''', (executor, token))
        return c.fetchone() is not None
    
    def has_pending_job(username):
        c = con.cursor()
        c.execute('''
        SELECT * FROM jobs
        WHERE username = ? AND state = 'pending'
        ''', (username,))
        return c.fetchone() is not None
    
    def submit_job(username, request_json):
        c = con.cursor()
        
        c.execute('''
        DELETE FROM jobs
        WHERE username = ? AND state = 'pending'
        ''', (username,))

        c.execute('''
        INSERT INTO jobs (username, submitted_at_iso_8601, request_json, state)
        VALUES (?, ?, ?, ?)
        ''', (username, curr_timestamp(), json.dumps(request_json), "pending"))
        
        job_id = c.lastrowid
        
        return job_id
    
    def delete_job(job_id):
        c = con.cursor()

        c.execute('''
        DELETE FROM jobs
        WHERE id = ?
        ''', (job_id,))

    def claim_job(executor):
        c = con.cursor()

        # Get the least recently served user that has a pending job. If there are users which have
        # never been served (i.e. last_served_iso_8601 is NULL) but which have a pending job in
        # the 'jobs' table, then they should be served first, in order of when they submitted their
        # pending job.
        c.execute('''
        SELECT u.username, j.id, j.request_json AS job_id
        FROM users u
        JOIN jobs j ON u.username = j.username
        LEFT JOIN priorities p ON u.username = p.username
        WHERE j.state = 'pending'
        ORDER BY 
            CASE
                WHEN (p.username is NULL OR p.last_served_iso_8601 IS NULL) THEN 0
                ELSE 1
            END ASC,
            CASE
                WHEN (p.username is NULL OR p.last_served_iso_8601 IS NULL) THEN j.submitted_at_iso_8601
                ELSE p.last_served_iso_8601
            END ASC,
            j.submitted_at_iso_8601 ASC
        LIMIT 1
        ''')

        result = c.fetchone()
        if result is None:
            return None, None
        
        username, job_id, request_json = result

        timestamp = curr_timestamp()

        # mark the job as claimed
        c.execute('''
        UPDATE jobs
        SET state = 'claimed', claimed_at_iso_8601 = ?
        WHERE id = ?
        ''', (timestamp, job_id))

        # update the last served timestamp for the user
        c.execute('''
        INSERT INTO priorities (username, last_served_iso_8601)
        VALUES (?, ?)
        ON CONFLICT(username) DO UPDATE SET last_served_iso_8601 = ?
        ''', (username, timestamp, timestamp))

        return job_id, request_json
    
    def complete_job(job_id, result_json):
        c = con.cursor()

        c.execute('''
        UPDATE jobs
        SET state = 'complete', completed_at_iso_8601 = ?, result_json = ?
        WHERE id = ?
        ''', (curr_timestamp(), json.dumps(result_json), job_id))
    
    def garbage_collect():
        c = con.cursor()

        current_time = datetime.now(timezone.utc).isoformat()

        # Delete completed jobs that are older than max_completed_job_age
        c.execute('''
        DELETE FROM jobs
        WHERE state = 'complete' AND
            datetime(completed_at_iso_8601) < datetime(?, '-' || ? || ' seconds')
        ''', (current_time, max_completed_job_age,))

        # Delete claimed jobs that are older than max_claimed_job_age
        c.execute('''
        DELETE FROM jobs
        WHERE state = 'claimed' AND
            datetime(claimed_at_iso_8601) < datetime(?, '-' || ? || ' seconds')
        ''', (current_time, max_claimed_job_age,))
    
    def get_job_status(job_id):
        # returns a tuple of (state, result_json)
        c = con.cursor()
        c.execute('''
        SELECT state, result_json FROM jobs
        WHERE id = ?
        ''', (job_id,))
        return c.fetchone()

    class Handler(http.server.BaseHTTPRequestHandler):
        def _set_headers(self, code=200):
            self.send_response(code)
            self.send_header('Content-type', 'application/json')
            self.end_headers()

        def _authenticate_user(self, username, token):
            if not authenticate_user(username, token):
                self._set_headers(401)
                self.wfile.write(json.dumps({
                    "error": "Invalid username or token"
                }).encode())
                return False
            return True

        def _authenticate_executor(self, executor, token):
            if not authenticate_executor(executor, token):
                self._set_headers(401)
                self.wfile.write(json.dumps({
                    "error": "Invalid executor or token"
                }).encode())
                return False
            return True
        
        def do_GET(self):
            try:
                # Get the path and query arguments from the URL
                url_parts = urlparse(self.path)
                path = url_parts.path
                query_args = parse_qs(url_parts.query)

                if path.startswith("/api/status"):
                    with con:
                        garbage_collect()

                        username = query_args["username"][0]
                        token = query_args["token"][0]
                        if not self._authenticate_user(username, token):
                            return

                        job_id = query_args["job_id"][0]

                        state, result_json = get_job_status(job_id)

                    self._set_headers()
                    self.wfile.write(json.dumps({
                        "success": True,
                        "state": state,
                        "result": result_json,
                    }).encode())
                    return
                else:
                    self._set_headers(404)
                    self.wfile.write(json.dumps({
                        "error": "Invalid path"
                    }).encode())
                    return
            except Exception as e:
                traceback.print_exc()
                self._set_headers(400)
                self.wfile.write(json.dumps({
                    "error": str(e)
                }).encode())

        def do_POST(self):
            try:
                # Get the path and query arguments from the URL
                url_parts = urlparse(self.path)
                path = url_parts.path
                query_args = parse_qs(url_parts.query)

                if path.startswith("/api/submit"):
                    with con:
                        garbage_collect()

                        username = query_args["username"][0]
                        token = query_args["token"][0]
                        if not self._authenticate_user(username, token):
                            return
                        
                        content_length = int(self.headers['Content-Length'])
                        if content_length > max_submit_size:
                            self._set_headers(400)
                            self.wfile.write(json.dumps({
                                "error": "Request too large"
                            }).encode())
                            return
                        post_data = self.rfile.read(content_length)
                        post_data = json.loads(post_data.decode())

                        audit_log.write(json.dumps({
                            "timestamp": curr_timestamp(),
                            "username": username,
                            "action": "submit",
                            "request": post_data,
                        }))
                        audit_log.write("\n")
                        audit_log.flush()

                        if has_pending_job(username):
                            if "override_pending" not in query_args:
                                self._set_headers(400)
                                self.wfile.write(json.dumps({
                                    "error": "pending_job"
                                }).encode())
                                return
                        
                        job_id = submit_job(username, post_data)

                    self._set_headers()
                    self.wfile.write(json.dumps({
                        "success": True,
                        "job_id": job_id,
                    }).encode())
                    return
                elif path.startswith("/api/delete"):
                    with con:
                        garbage_collect()

                        username = query_args["username"][0]
                        token = query_args["token"][0]
                        if not self._authenticate_user(username, token):
                            return
                        
                        job_id = query_args["job_id"][0]

                        delete_job(job_id)

                    self._set_headers()
                    self.wfile.write(json.dumps({
                        "success": True,
                    }).encode())
                    return
                elif path.startswith("/api/claim"):
                    with con:
                        garbage_collect()

                        executor = query_args["executor"][0]
                        token = query_args["token"][0]

                        if not self._authenticate_executor(executor, token):
                            return
                        
                        job_id, request_json = claim_job(executor)

                    self._set_headers()
                    if job_id is not None:
                        self.wfile.write(json.dumps({
                            "success": True,
                            "job_id": job_id,
                            "request_json": json.loads(request_json),
                        }).encode())
                    else:
                        self.wfile.write(json.dumps({
                            "success": True,
                            "job_id": None,
                        }).encode())
                    return
                elif path.startswith("/api/complete"):
                    with con:
                        garbage_collect()

                        executor = query_args["executor"][0]
                        token = query_args["token"][0]

                        if not self._authenticate_executor(executor, token):
                            return
                        
                        job_id = query_args["job_id"][0]
                        content_length = int(self.headers['Content-Length'])
                        post_data = self.rfile.read(content_length)
                        post_data = json.loads(post_data.decode())

                        complete_job(job_id, post_data)

                    self._set_headers()
                    self.wfile.write(json.dumps({
                        "success": True,
                    }).encode())
                    return
                else:
                    self._set_headers(404)
                    self.wfile.write(json.dumps({
                        "error": "Invalid path"
                    }).encode())
                    return
            except Exception as e:
                traceback.print_exc()
                self._set_headers(400)
                self.wfile.write(json.dumps({
                    "error": str(e)
                }).encode())

    port = 4443

    httpd = http.server.HTTPServer(('', port), Handler)

    cert_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cert")

    certfile = os.path.join(cert_dir, "server.crt")
    keyfile = os.path.join(cert_dir, "server.key")

    httpd.socket = ssl.wrap_socket(
        httpd.socket,
        certfile=certfile,
        keyfile=keyfile,
        server_side=True,
    )

    print("serving at port", port)
    httpd.serve_forever()

if __name__ == "__main__":
    main()