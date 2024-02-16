# Telerun Job Queue System

## Submission Queue

The submission queue acts as the central manager for Telerun, and is intended to be run on a low-power machine with at least one exposed port available for use by an HTTPS server.

The submission queue requires paired SSL certificate and key files to be present at the following paths relative to the project root:
* `queue/server.cert`
* `queue/server.key`

To launch the submission queue server, run:

```bash
$ cd queue/
$ python3 -m submission_queue.main
```

To administer the authentication database, run:

```bash
$ cd queue/
$ python3 -m submission_queue.auth <arguments...>
```

To administer non-authentication functions of the job queue database, run:

```bash
$ cd queue/
$ python3 -m submission_queue.db <arguments...>
```

The submission queue will create and manage the following persistent files:
* `queue/audit_log.jsonl`
    * This stores the source code of every program submitted to the queue.
* `queue/db.sqlite3`
    * This stores user identity information and job queue state.

## Executor

The executor is responsible for compiling and running programs submitted to the queue. It operates by continuously polling the submission queue for available work, and sending results back to the queue once they are available. The executor is intended to be run on a high-power machine equipped with at least one GPU. The executor only makes outbound HTTPS requests, and does not require any ports to be exposed to the public internet. It is possible to run multiple executors simultaneously on different machines to scale the capacity of the system horizontally.

The executor is distributed as a single-file Python application, `executor.py`. This script does not depend on any third-party Python libraries, although it does expect `nvcc` to be installed.

The executor script requires a file `auth.json` to be present in the same directory as the script. This `auth.json` file should be structured as follows:
```json
{
    "executor": /* executor name... */,
    "token": /* executor token... */
}
```

The executor can then be run as follows:
```
$ python3 executor.py --nproc-compile <number> --nproc-execute <number>
```
The arguments `--nproc-compile` and `--nproc-execute` determine the number of parallel workers the executor will spawn for the compilation and execution stages of the pipeline, respectively. Each execution worker will be granted exclusive access to a single GPU via the `CUDA_VISIBLE_DEVICES` environment variable. The number of execution workers should not exceed the number of available GPUs.

## Client

The client allows users of the system to submit jobs to the queue and see their results. Like the executor, the client is distributed as a single-file zero-dependency Python application, `submit.py`.

The client requires an `auth.json` file to be present in the same directory as the script, structured as follows:
```json
{
    "username": /* username... */,
    "token": /* user token... */
}
```

The client can then be run as:
```
$ python3 submit.py <local .cu source file>
```
After submitting the provided source file to the submission queue, the client will continuously poll the queue until the results of compiling and running the submitted program are available, at which point it prints the results and exits.