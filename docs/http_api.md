### Mist Api V2

Glosssary:

- Endpoint (previously was routers) - thing that describe job (path to artifact, class name, job parameters)
- Job - fact of endpoint invocation
- Context (namespace) - group of spark configs or settings for SparkContext creation
- Mode - job can be runned in two worker modes: `shared` or `exclusive`
    - Shared - all jobs from same context are using one spark driver application
    - Exclusive - fresh driver aplication will be created for one job invocation

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
      <td>/v2/api/endpoints/{id}</td>
      <td>
        <p>Post data: endpoint (MistJob) arguments </p>
        <p>Query params:
          <ul>
            <li>context - Not required, specify contextId/namespace/spark conf </li>
            <li>mode - Not required, values: [exlusive|shared]</li>
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
        </ul>
      </td>
      <td>List of jobs that was runned on that endpoint</td>
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
