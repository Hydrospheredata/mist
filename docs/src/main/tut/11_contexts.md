---
layout: docs
title: "Contexts"
permalink: contexts.html
position: 11
---
## Contexts 

Mist creates and orchestrates Apache Spark contexts automatically. Every job is run in a context.
In fact context describes a named Spark context and Mist settings for this Spark context.
Mist context settings:
- `spark-conf` - settings for a [spark](https://spark.apache.org/docs/latest/configuration.html)
- `max-paralle-jobs` - amount of jobs that can be executed in parallel on the same context
- `run-options` - additional option with command line arguments that will be used during worker creationa using spark-submit.
   by default it empty. [spark docs](https://spark.apache.org/docs/latest/submitting-applications.html)
- `streaming-duration` - spark streaming duration 
- `worker-mode`:
    There are two types of modes:
    - `shared`:
        By default when you request a job to run the first time, Mist creates a worker node with new Spark context.
        The second request will use the created namespace so the context will be alive while Mist is running.
        settings:
        - `downtime`(durarion) - idle-timeout for `shared` worker
        - `precreated`(boolean) - start context at mist startup time
  
    - `exclusive` - spawn new driver application for every job invocation.
