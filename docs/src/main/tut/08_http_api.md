---
layout: docs
title: "Http Api"
permalink: http_api.html
position: 8
---
#### Http api

**Functions**:
<table>
  <thead>
    <tr>
      <td>Method</td>
      <td>Path</td>
      <td>Params</td>
      <td>Description</td>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>GET</td>
      <td>/v2/api/functions</td>
      <td>None</td>
      <td>List of all functions</td>
    </tr>
    <tr>
      <td>GET</td>
      <td>/v2/api/functions/{id}</td>
      <td>None</td>
      <td>Get function by id</td>
    </tr>
    <tr>
      <td>DELETE</td>
      <td>/v2/api/functions/{id}</td>
      <td>None</td>
      <td>Delete function by id</td>
    </tr>
    <tr>
      <td>POST</td>
      <td>/v2/api/functions</td>
      <td>
        <p>Post body - json: function configuration:
          <ul>
            <li>name</li>
            <li>path</li>
            <li>className</li>
            <li>defaultContext</li>
          </ul>
        </p>
      </td>
      <td>Create function</td>
    </tr>
    <tr>
      <td>POST</td>
      <td>/v2/api/functions/{id}/jobs</td>
      <td>
        <p>Post body: function (MistFn) arguments </p>
        <p>Query params:
          <ul>
            <li>context - optional, specify contextId/namespace/spark conf </li>
            <li>startTimeout - optional (default Inf) - max time for staying in queue (e.g: 10s, 10ms, 10minute)</li>
            <li>performTimeout - optional (default Inf) - max time for invocation (e.g: 10s, 10ms, 10minute)</li>
          </ul>
        </p>
      </td>
      <td>Start job on function</td>
    </tr>
    <tr>
      <td>GET</td>
      <td>/v2/api/functions/{id}/jobs</td>
      <td>Query params:
        <ul>
          <li>limit - optional (default 25)</li>
          <li>offset - optional (default 0)</li>
          <li>status - optional, repeated (values: started, finished ... )</li>
          <li>paginate - boolean (default false) - response with struct `{"jobs": [array of jobs], "total": int}`</li>
        </ul>
      </td>
      <td>List of jobs that was ran with given function</td>
    </tr>

  </tbody>
</table>

**Jobs**:
<table>
  <thead>
    <tr>
      <td>Method</td>
      <td>Path</td>
      <td>Params</td>
      <td>Description</td>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>GET</td>
      <td>/v2/api/jobs</td>
      <td>Query params:
        <ul>
          <li>limit - optional (default 25)</li>
          <li>offset - optional (default 0)</li>
          <li>status - optional, repeated (values: started, finished ... )</li>
          <li>paginate - boolean (default false) - response with struct `{"jobs": [array of jobs], "total": int}`</li>
        </ul>
      </td>
      <td>List of all jobs that was ran on all functions</td>
    </tr>
    <tr>
      <td>GET</td>
      <td>/v2/api/jobs/{id}</td>
      <td>None</td>
      <td>Get info about particular job</td>
    </tr>
    <tr>
      <td>GET</td>
      <td>/v2/api/jobs/{id}/logs</td>
      <td>None</td>
      <td>Logs from job</td>
    </tr>
    <tr>
      <td>DELETE</td>
      <td>/v2/api/jobs/{id}</td>
      <td>None</td>
      <td>Try cancel job execution</td>
    </tr>
  </tbody>
</table>

**Workers**:
<table>
  <thead>
    <tr>
      <td>Method</td>
      <td>Path</td>
      <td>Params</td>
      <td>Description</td>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>GET</td>
      <td>/v2/api/workers</td>
      <td>None</td>
      <td>List of all active workers</td>
    </tr>
    <tr>
      <td>DELETE</td>
      <td>/v2/api/workers/{id}</td>
      <td>None</td>
      <td>Stop worker</td>
    </tr>
    <tr>
      <td>GET</td>
      <td>/v2/api/workers/{id}</td>
      <td>None</td>
      <td>Get detailed worker info(context config, jobs, etc..)</td>
    </tr>
    <tr>
      <td>GET</td>
      <td>/v2/api/workers/{id}/jobs</td>
      <td>Query params:
        <ul>
          <li>limit - optional (default 25)</li>
          <li>offset - optional (default 0)</li>
          <li>status - optional, repeated (values: started, finished ... )</li>
          <li>paginate - boolean (default false) - response with struct `{"jobs": [array of jobs], "total": int}`</li>
        </ul>
      </td>
      <td>List of all jobs that was ran on worker</td>
    </tr>
  </tbody>
</table>

**Contexts**:
<table>
  <thead>
    <tr>
      <td>Method</td>
      <td>Path</td>
      <td>Params</td>
      <td>Description</td>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>GET</td>
      <td>/v2/api/contexts</td>
      <td>None</td>
      <td>List of all contexts</td>
    </tr>
    <tr>
      <td>GET</td>
      <td>/v2/api/contexts/{id}</td>
      <td>None</td>
      <td>Get context by id</td>
    </tr>
    <tr>
      <td>DELETE</td>
      <td>/v2/api/contexts/{id}</td>
      <td>None</td>
      <td>Delete context by id</td>
    </tr>
    <tr>
      <td>POST</td>
      <td>/v2/api/contexts</td>
      <td>Json body:
        <ul>
          <li>name</li>
          <li>sparkConf - Key-value string->string object</li>
          <li>downtime - Idle timeout before worker shut self down - Duration</li>
          <li>maxJobs - max parallel jobs - Int</li>
          <li>workerMode - worker mode (shared | exclusive) - String</li>
          <li>precreated - Boolean</li>
          <li>runOptions - String</li>
          <li>streamingDuration - Duration</li>
        </ul>
      </td>
      <td>Get context by id</td>
    </tr>
  </tbody>
</table>

**Status**
<table>
  <thead>
    <tr>
      <td>Method</td>
      <td>Path</td>
      <td>Params</td>
      <td>Description</td>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>GET</td>
      <td>/v2/api/status</td>
      <td>None</td>
      <td>Mist status info (version, start time, spark version)</td>
    </tr>
  </tbody>
</table>

**Artifacts**
<table>
  <thead>
    <tr>
      <td>Method</td>
      <td>Path</td>
      <td>Params</td>
      <td>Description</td>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>GET</td>
      <td>/v2/api/artifacts</td>
      <td>None</td>
      <td>list of all artifacts</td>
    </tr>
    <tr>
      <td>GET</td>
      <td>/v2/api/artifacts/{id}</td>
      <td>None</td>
      <td>artifact info</td>
    </tr>
    <tr>
      <td>DELETE</td>
      <td>/v2/api/artifacts/{id}</td>
      <td>None</td>
      <td>Delete artifact</td>
    </tr>
     <tr>
      <td>POST</td>
      <td>/v2/api/artifacts</td>
      <td>file: Multipart form data</td>
      <td>upload new artifact</td>
    </tr>  
  </tbody>
</table>


Note: Mist always has default context settings, you can obtain it by "default" id.
