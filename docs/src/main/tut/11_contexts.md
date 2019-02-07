---
layout: docs
title: "Contexts"
permalink: contexts.html
position: 11
---
### Contexts

Mist creates and orchestrates Apache Spark contexts automatically. Every job is run in a context.
In fact context describes a named Spark context and Mist settings for this Spark context.

Contexts may be created using [mist-cli](/mist-docs/mist-cli.html) or [http-api](/mist-docs/http_api.html).
Also, there is special `default` context. It may be configured only using mist-configuration file.
It's goal to setup default values for all context, so for creating a new context it isn't required to define values for all its fields.

Settings:
<table>
  <thead>
    <tr>
       <td>Key</td>
       <td>Default</td>
       <td>Meaning</td>
    </tr>
  </thead>
  <tbody>
    <tr>
       <td>sparkConf</td>
       <td>empty</td>
       <td>settings for a [spark](https://spark.apache.org/docs/latest/configuration.html)</td>
    </tr>
    <tr>
       <td>maxJobs</td>
       <td>1</td>
       <td>amount of jobs executed in parallel</td>
    </tr>
    <tr>
       <td>workerMode</td>
       <td>exclusive</td>
       <td>
         <ul>
           <li>exclusive - starts new worker for every job invocation</li>
           <li>shared - reuses worker between several jobs</li>
         </ul>
       </td>
    </tr>
    <tr>
       <td>maxConnFailures</td>
       <td>5</td>
       <td>
         allowed amount of worker crushes before context will be switched into `broken` state
         (it fails all incoming requests until context settings is updated).
       </td>
    </tr>
    <tr>
       <td>runOptions</td>
       <td>""</td>
       <td>
         additional command line arguments for building spark-submit command to start worker, e.x: pass `--jars`
       </td>
    </tr>
    <tr>
       <td>streamingDuration</td>
       <td>1s</td>
       <td>
         spark streaming duration
       </td>
    </tr>
    <tr>
       <td>precreated</td>
       <td>false</td>
       <td>
         if true starts worker immediately,
         if false await first job start requests before starting worker
         *NOTE*: works only with `shared` workerMode
       </td>
    </tr>
    <tr>
       <td>downtime</td>
       <td>infinity</td>
       <td>idle-timeout for `shared` worker</td>
    </tr>
  </tbody>
</table>
