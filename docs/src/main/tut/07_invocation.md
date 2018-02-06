---
layout: docs
title: "How Mist invokes functions"
permalink: invocation.html
position: 7
---

### Invocation

Job could be in one of possible statuses:
- `queued`
- `started` 
- `canceled`
- `finished`
- `failed`

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
