import urllib
import urllib.parse
import urllib.request
import ssl
import os
import json
import time
import traceback
import multiprocessing
import subprocess
from dataclasses import dataclass
from typing import Optional
import argparse
import uuid
import shutil

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

compile_timeout = 60 # seconds
execute_timeout = 60 # seconds

max_log_length = 1 << 20

compute_capability = "86"

@dataclass
class CompileJob:
    job_id: int
    job_dir: str
    source: str

@dataclass
class ExecuteJob:
    job_id: int
    job_dir: str
    compile_log: str

@dataclass
class CompleteJob:
    job_id: int
    job_dir: str
    success: bool
    compile_log: str
    execute_log: Optional[str] = None

def src_path(job_dir: str) -> str:
    return os.path.join(job_dir, "src.cu")

def bin_path(job_dir: str) -> str:
    return os.path.join(job_dir, "bin")

def claim_worker(compile_queue, auth, scratch_dir: str):
    ssl_ctx = ssl.create_default_context(cadata=server_cert)
    
    auth_name = auth["executor"]
    auth_token = auth["token"]

    while True:
        time.sleep(poll_interval)
        try:
            url_query = urllib.parse.urlencode({"executor": auth_name, "token": auth_token})

            req = urllib.request.Request(
                "https://" + server_ip_port + "/api/claim?" + url_query,
                method="POST",
            )
            with urllib.request.urlopen(req, context=ssl_ctx) as f:
                response = json.load(f)
            
            assert response["success"]

            job_id = response["job_id"]
            if job_id is None:
                continue
            print("Claimed job", job_id)

            source = response["request_json"]["source"]

            job_dir = os.path.join(scratch_dir, str(f"job-{job_id}"))

            compile_queue.put(CompileJob(job_id, job_dir, source))
        except Exception as e:
            traceback.print_exc()
            continue

def compile_worker(compile_queue, complete_queue, execute_queue):
    while True:
        compile_job: CompileJob = compile_queue.get()
        put_fail = (
            lambda log: complete_queue.put(
                CompleteJob(compile_job.job_id, compile_job.job_dir, False, log, None)
            )
        )
        try:
            os.makedirs(compile_job.job_dir, exist_ok=True)
            with open(src_path(compile_job.job_dir), "w") as f:
                f.write(compile_job.source)
            out = subprocess.run(
                [
                    "nvcc",
                    "-O3",
                    "-use_fast_math",
                    f"-arch=compute_{compute_capability}",
                    f"-code=sm_{compute_capability}",
                    "-o", bin_path(compile_job.job_dir),
                    src_path(compile_job.job_dir),
                ],
                timeout=compile_timeout,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            if out.returncode == 0:
                execute_queue.put(ExecuteJob(compile_job.job_id, compile_job.job_dir, out.stdout))
            else:
                put_fail(out.stdout)
        except subprocess.TimeoutExpired:
            put_fail(
                f"Compilation timed out after {compile_timeout} seconds. Output log:\n\n" + out.stdout
            )
        except Exception as e:
            put_fail("Compilation failed with exception:\n" + str(e))

def execute_worker(execute_queue, complete_queue, gpu_index: int):
    while True:
        execute_job: ExecuteJob = execute_queue.get()
        put_complete = (
            lambda success, log: complete_queue.put(
                CompleteJob(execute_job.job_id, execute_job.job_dir, success, execute_job.compile_log, log)
            )
        )
        try:
            out = subprocess.run(
                [bin_path(execute_job.job_dir)],
                timeout=execute_timeout,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env={**os.environ, "CUDA_VISIBLE_DEVICES": str(gpu_index)},
                cwd=execute_job.job_dir,
            )
            if out.returncode == 0:
                put_complete(True, out.stdout)
            else:
                put_complete(False, out.stdout)
        except subprocess.TimeoutExpired:
            put_complete(
                False,
                f"Execution timed out after {execute_timeout} seconds. Output log:\n\n" + out.stdout,
            )
        except Exception as e:
            put_complete(False, "Execution failed with exception:\n" + str(e))

def truncate_text(text, max_length):
    if len(text) > max_length:
        return text[:max_length // 2] + "\n--- truncated... ---\n" + text[-max_length // 2:]
    else:
        return text

def complete_worker(complete_queue, auth):
    ssl_ctx = ssl.create_default_context(cadata=server_cert)

    auth_name = auth["executor"]
    auth_token = auth["token"]

    while True:
        completion: CompleteJob = complete_queue.get()
        try:
            shutil.rmtree(completion.job_dir, ignore_errors=True)

            completion_req_query = urllib.parse.urlencode({"executor": auth_name, "token": auth_token, "job_id": completion.job_id})

            if completion.execute_log is None:
                completion.execute_log = ""

            completion_data = json.dumps({
                "result_json": {
                    "success": completion.success,
                    "compile_log": truncate_text(completion.compile_log, max_log_length),
                    "execute_log": truncate_text(completion.execute_log, max_log_length),
                }
            }).encode("utf-8")

            completion_req = urllib.request.Request(
                "https://" + server_ip_port + "/api/complete?" + completion_req_query,
                data=completion_data,
                method="POST",
            )
            with urllib.request.urlopen(completion_req, context=ssl_ctx) as f:
                pass
        except Exception as e:
            traceback.print_exc()
            continue

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nproc-compile", type=int, required=True)
    parser.add_argument("--nproc-execute", type=int, required=True)
    parser.add_argument(
        "--auth",
        help="Authentication token (defaults to ./auth.json in the same directory as this script)",
    )
    parser.add_argument(
        "--scratch-dir",
        help="Directory to store temporary files (default: /tmp)",
        default="/tmp",
    )
    args = parser.parse_args()

    token_path = args.auth or os.path.join(os.path.dirname(__file__), "auth.json")
    with open(token_path, "r") as f:
        auth = json.load(f)
    
    scratch_uuid = str(uuid.uuid4())
    scratch_dir = os.path.join(args.scratch_dir, f"executor-{scratch_uuid}")
    os.makedirs(scratch_dir, exist_ok=True)

    compile_queue = multiprocessing.Queue(1)
    execute_queue = multiprocessing.Queue(1)
    complete_queue = multiprocessing.Queue(1)

    claim_proc = multiprocessing.Process(target=claim_worker, args=(compile_queue, auth, scratch_dir))
    claim_proc.start()

    compile_procs = [
        multiprocessing.Process(target=compile_worker, args=(compile_queue, complete_queue, execute_queue))
        for _ in range(args.nproc_compile)
    ]
    for proc in compile_procs:
        proc.start()
    
    execute_procs = [
        multiprocessing.Process(target=execute_worker, args=(execute_queue, complete_queue, i))
        for i in range(args.nproc_execute)
    ]
    for proc in execute_procs:
        proc.start()
    
    complete_proc = multiprocessing.Process(target=complete_worker, args=(complete_queue, auth))
    complete_proc.start()

    claim_proc.join()


if __name__ == "__main__":
    main()