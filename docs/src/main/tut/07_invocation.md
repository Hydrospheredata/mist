---
layout: docs
title: "How Mist invokes functions"
permalink: invocation.html
position: 7
---

### Invocation model

Mist treats every function invocation as a job.
To invoke its mist has to have a spark-driver application with spark-context.
This application called `mist-worker` and mist starts and manage them automatically.

Before starting job execution mist master queues them
and waits when there will be a free worker for a context where the job should be executed.
There are two worker-modes for contexts: `exclusive` and `shared`.
For exclusive worker-mode mist starts a new worker per every job and shuts it down after its completion.
For shared mist doesn't shutdown workers right after job completion, it reuses them to execute next jobs that should be executed in the same context.

Also, this invocation model provides following benefits:
- **Parallelization**:
  For a particular context it's possible to start more than one `mist-worker` at the same time and balance job requests between them
  to provide a way to execute jobs in parallel. Parallelization level is controlled by `max-parallel-jobs` in contexts settings - [contexts doc](/mist-docs/contexts.html)
- **Fault-tolerance**:
  If a worker was crushed mists tries to restart it to invoke the next job.
  If workers were crushed more than `maxConnFailures` ([contexts doc](/mist-docs/contexts.html)) in sequence this context marks as broken
  and fails all jobs until it will be updated.

### Job statuses

Job may be in one of possible statuses:
- `queued`
- `started` 
- `canceled`
- `finished`
- `failed`

### Steps

Steps on Mist after receiving a new job request:
- assign `id` fo job.
  For example if you use http:
  ```sh
  $ curl -d '{"numbers": [1, 2, 3]}' "http://localhost:2004/v2/api/functions/spark-ctx-example/jobs"
  # response:
  # {"id":"2a1c90c5-8eb7-4bde-84a2-d176147fc24f"}
  ```
- validate input parameters locally. If they fit for target function job will be queued, otherwise job will be failed
- check and prepare worker:
    - `shared` - check that there is worker app exists or spawn them
    - `exclusive` - always spawn new worker

  Possible problems: a new worker creation has default creation timeout(see [configuration](/mist-docs/configuration.html)).
  If worker wasn't registered on master during that timeout, job will be failed.
  
- transmit function artifact to worker(jar/py)
- invoke function. status turns into `started` and then after compeletion `finished` or `failed`

