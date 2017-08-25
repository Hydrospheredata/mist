### Mist Api V2

Glosssary:

- Endpoint (previously was routers) - thing that describe job (path to artifact, class name, job parameters)
- Job - fact of endpoint invocation
- Namespace - SparkContext configs and Mist configs for given SparkContext 
- Worker - Mist slave instance that holds Spark Driver application
- Mode - job can be run in two worker modes: `shared` or `exclusive` ([context configuration](configuration.md))
    - Shared - all jobs from same context are using one spark driver application
    - Exclusive - fresh driver application will be created for one job invocation

#### Rest

**Endpoints**:
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
      <td>/v2/api/endpoints</td>
      <td>None</td>
      <td>List of all endpoints</td>
    </tr>
    <tr>
      <td>GET</td>
      <td>/v2/api/endpoints/{id}</td>
      <td>None</td>
      <td>Get endpoint by id</td>
    </tr>
    <tr>
      <td>POST</td>
      <td>/v2/api/endpoints</td>
      <td>
        <p>Post body - json: endpoint configuration:
          <ul>
            <li>name</li>
            <li>path</li>
            <li>className</li>
            <li>defaultContext</li>
          </ul>
        </p>
      </td>
      <td>Create endpoint</td>
    </tr>
    <tr>
      <td>POST</td>
      <td>/v2/api/endpoints/{id}/jobs</td>
      <td>
        <p>Post body: endpoint (MistJob) arguments </p>
        <p>Query params:
          <ul>
            <li>context - Not required, specify contextId/namespace/spark conf </li>
            <li>workerId - Not required</li>
          </ul>
        </p>
      </td>
      <td>Start job on endpoint</td>
    </tr>
    <tr>
      <td>GET</td>
      <td>/v2/api/endpoints/{id}/jobs</td>
      <td>Query params:
        <ul>
          <li>limit - optional (default 25)</li>
          <li>offset - optional (default 0)</li>
          <li>status - optional, repeated (values: started, finished ... )</li>
        </ul>
      </td>
      <td>List of jobs that was run with given endpoint</td>
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
        </ul>
      </td>
      <td>List of all jobs that was runned on all endpoints</td>
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


Note: Mist always has default context settings, you can obtain it by "default" id.



### Next 
- [Reactive API](/docs/reactive_api.md)
- [CLI](/docs/cli.md)
