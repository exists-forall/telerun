import sqlite3
import os
import argparse

def init_auth_schema(cur):
    cur.execute('''
    CREATE TABLE IF NOT EXISTS users (
        username TEXT NOT NULL PRIMARY KEY,
        token TEXT NOT NULL
    )
    ''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS executors (
        name TEXT NOT NULL PRIMARY KEY,
        token TEXT NOT NULL
    )
    ''')

def init_submission_schema(cur):
    cur.execute('''
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER NOT NULL PRIMARY KEY,
        username TEXT NOT NULL,
        submitted_at_iso_8601 TEXT NOT NULL,
        claimed_at_iso_8601 TEXT,
        completed_at_iso_8601 TEXT,
        request_json TEXT NOT NULL,
        result_json TEXT,
        state TEXT NOT NULL,
        FOREIGN KEY(username) REFERENCES users(username) ON DELETE CASCADE,
        CHECK (
            (state = 'pending' AND
                claimed_at_iso_8601 IS NULL AND
                completed_at_iso_8601 IS NULL AND
                result_json IS NULL
            ) OR
            (state = 'claimed' AND
                claimed_at_iso_8601 IS NOT NULL AND
                completed_at_iso_8601 IS NULL AND
                result_json IS NULL
            ) OR
            (state = 'complete' AND
                claimed_at_iso_8601 IS NOT NULL AND
                completed_at_iso_8601 IS NOT NULL AND
                result_json IS NOT NULL
            )
        )
    )
    ''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS priorities (
        username TEXT NOT NULL UNIQUE,
        last_served_iso_8601 TEXT,
        FOREIGN KEY(username) REFERENCES users(username) ON DELETE CASCADE
    )
    ''')

def reset_submissions(cur):
    # drop submissions and priorities tables
    cur.execute('''
    DROP TABLE IF EXISTS jobs
    ''')
    cur.execute('''
    DROP TABLE IF EXISTS priorities
    ''')
    # recreate submissions and priorities tables
    init_submission_schema(cur)

def init_schema(con):
    c = con.cursor()
    init_auth_schema(c)
    init_submission_schema(c)
    con.commit()

def connect_to_db_no_init():
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db.sqlite3')
    conn = sqlite3.connect(db_path)
    init_schema(conn)
    return conn

def connect_to_db():
    conn = connect_to_db_no_init()
    init_schema(conn)
    return conn

def init_db_handler(args):
    conn = connect_to_db()
    conn.close()

def reset_submissions_handler(args):
    conn = connect_to_db_no_init()
    reset_submissions(conn.cursor())
    conn.close()

def main():
    parser = argparse.ArgumentParser(description='Submission Queue CLI')
    subparsers = parser.add_subparsers(title='subcommands', dest='subcommand')

    # init-db subcommand
    init_db_parser = subparsers.add_parser('init-db', help='Initialize the database schema')
    init_db_parser.set_defaults(func=init_db_handler)

    # reset-submissions subcommand
    reset_submissions_parser = subparsers.add_parser('reset-submissions', help='Reset the submissions table')
    reset_submissions_parser.set_defaults(func=reset_submissions_handler)

    args = parser.parse_args()
    if hasattr(args, 'func'):
        args.func(args)

if __name__ == '__main__':
    main()