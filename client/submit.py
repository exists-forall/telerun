import argparse
import urllib
import urllib.parse
import urllib.request
import ssl
import os
import json
import traceback
import time

server_cert = """
-----BEGIN CERTIFICATE-----
MIIFIjCCAwqgAwIBAgIUW4PyhbrTxMoQYOeFOIjFc6iQOsAwDQYJKoZIhvcNAQEL
BQAwGDEWMBQGA1UEAwwNMzUuMjExLjM3LjIzMjAeFw0yNDAxMjcwNjA1NTFaFw0y
NTAxMjYwNjA1NTFaMBgxFjAUBgNVBAMMDTM1LjIxMS4zNy4yMzIwggIiMA0GCSqG
SIb3DQEBAQUAA4ICDwAwggIKAoICAQDAzrIuTX6VH//GGAd5jBBmt+G6+lmqvSrR
f27qj0C9PmV9OsATRseWg1d6EcxHxRZyoGEFedr2HDTgQSIyA0gWFXCyBMLRUriy
HtQ4jkvVtH083RU4TJAiqYMtSztaAzaSpWsK/MOc3VbEaEHknWPK2lX0TO5qQ3SR
VRa4CaKufljA7uN8dWc9BMnkg8JQqY2Ipb31M0h/RmHSaDvTdFNx+7oSPKOQE+Z3
+PzqxMt2seFj8RTPnNJVVSuD+XpgaqKmRxcVHVaHBieCgS3XTj4UzU5nFk9bprjH
KEoKJxLfOnBOfJG4fcIeVkOuzDUZ8m2U2cKFLbAJNPnlaKcrhLe7EQkuc4L0SmVO
58BAL8jdbI1RyVXLCXgb/54pu6ZnMW42Ta48ELYaI2Q+AvYfOEpOlLz4voTDNUw6
BXlvn/e7p/w1kf5wPFxqaxB4pykF4ykakxUa3ZCFM3m5VXsIVy//FNBMeXRAbcF4
/gbu1jvrZVqdtsy3eB1k06Yu81IL1sU0So5LfsJsdlgESFyuzMrIbwMN6sJNQJb9
i2a22ddS6p2LXJvasbj5zwbymwEMzrUW0AxBtJauPeD1n/FTVQkS5Ra4qvyFAFYu
LGqrw5siOlOMEY8IzeqPnbrJsYBIS7jstoJCJkfKX+OmQBJgRZyqPQ3ay18d75vx
UYLTi8A3rQIDAQABo2QwYjAdBgNVHQ4EFgQUC6gVoka9Wb5RLPk9P1ALwvYGo60w
HwYDVR0jBBgwFoAUC6gVoka9Wb5RLPk9P1ALwvYGo60wDwYDVR0TAQH/BAUwAwEB
/zAPBgNVHREECDAGhwQj0yXoMA0GCSqGSIb3DQEBCwUAA4ICAQCq4vnyhiN2CiDA
4Lah5Ug9hPfIq+oPfR4l9irAJkJS8I8/LxsOiQeJOquv/ZlmNKtgcwxSQC6GBsuS
X+OkgRWaqI+RzAcFKalKyVouXyJss0lZ80mO4jUS5gC2HtBqflxeoOUAGVYMqReU
soEaPtJIqyMAZ7Sj3G9KpXRAYYYV9UqloBH2hKWcdLyUGRstua+qSMX1obeBpvQc
UZ646kuKroucQngV2w7EmMdJPn0sS/d/pCBIRhKrfkkITkAEcEJe6Z9QbJeVnqJh
uo22SYJMXfmPkPX25HlC3KqjPCKeUOIq7hK6xMYyftPh9flazrSCFL+raYvjW9ME
xMxw3rNZkIZDPdoOgHacjWZ0epRPHe/oAD1Y1mKCPJRFRzG027Alo0gIFPzfOMGg
1U1RR112jvegCyaG8pkGLdIg4PHcvBmMdocJ9grzzkS+w06KEKDQ5tTq99JDhkw6
1HsvPkKtF13efc2Q5sGcinKwaWldvj1bkRtTxItyQ9ptFwX2lFtKKXgklZwXtIog
hovmeaLzn0a2DRyf0WT4FHxf5p87qAX3E/1UDjkNEfmS65CBbF/V8sWP2LwY2V06
ph1s/l9nNrwG7T4ykdlEEPbilqR53qQIwkqzffPhtyMmah0nz/deFnUNv8szMng4
ZMKwWdP7qGf8yhCPY0LgJmvN1Apkag==
-----END CERTIFICATE-----
"""

server_ip_port = "35.211.37.232:4443"

poll_interval = 0.25 # seconds

def submit_job(username, token, source, ssl_ctx, override_pending=False):
    query_params = {"username": username, "token": token}
    if override_pending:
        query_params["override_pending"] = "1"
    url_query = urllib.parse.urlencode(query_params)
    url = "https://" + server_ip_port + "/api/submit?" + url_query
    req_json = json.dumps({"source": source}).encode("utf-8")
    request = urllib.request.Request(url, data=req_json, method="POST")
    request.add_header("Content-Type", "application/json")
    try:
        response = urllib.request.urlopen(request, context=ssl_ctx)
        response_json = json.load(response)
        return response_json["job_id"]
    except urllib.error.HTTPError as e:
        if e.code == 400:
            response_json = json.load(e)
            if response_json["error"] == "pending_job":
                return None
        raise e

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="CUDA source file to submit")
    parser.add_argument(
        "--auth",
        help="Authentication token (defaults to ./auth.json in the same directory as this script)",
    )
    parser.add_argument("--override-pending", action="store_true", help="Allow overriding pending jobs")
    args = parser.parse_args()

    token_path = args.auth or os.path.join(os.path.dirname(__file__), "auth.json")
    with open(token_path, "r") as f:
        auth = json.load(f)
    username = auth["username"]
    token = auth["token"]

    with open(args.file, "r") as f:
        source = f.read()
    ssl_ctx = ssl.create_default_context(cadata=server_cert)
    job_id = submit_job(username, token, source, ssl_ctx, override_pending=args.override_pending)
    if job_id is None:
        print("You already have a pending job. Pass '--override-pending' if you want to replace it.")
        exit(1)
    
    print("Submitted job", job_id)

    already_claimed = False

    while True:
        time.sleep(poll_interval)
        try:
            url_query = urllib.parse.urlencode({"username": username, "token": token, "job_id": job_id})
            req = urllib.request.Request(
                "https://" + server_ip_port + "/api/status?" + url_query,
                method="GET",
            )
            with urllib.request.urlopen(req, context=ssl_ctx) as f:
                response = json.load(f)
            
            state = response["state"]
            if state == "pending":
                continue
            elif state == "claimed":
                if not already_claimed:
                    print("Compiling and running")
                    already_claimed = True
                continue
            elif state == "complete":
                # TODO: Don't double-nest JSON!
                result = json.loads(response["result"])["result_json"]
                if result["success"]:
                    print("Job completed successfully.")
                else:
                    print("Job failed.")
                print()
                print("--- Compilation log:")
                print()
                print(result["compile_log"])
                print()
                print("--- Execution log:")
                print()
                print(result["execute_log"])
                break
        except Exception as e:
            traceback.print_exc()
            continue

if __name__ == "__main__":
    main()
