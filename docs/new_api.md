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


#### Livecycle events

There were several async interfaces described bellow, that can listen livecycle events from jobs.
All events have 2 required fields: `event`, `id`, and some of them provide more:
```js
{
  "event": "event-name",
  "id": "job id"
}
```

- `initialized` - job has been assigned to invocation, also returns all job parameters
- `queued` - job has been queued (start worker awaiting )
- `started` - job has been sended to worker
- `canceled` - job has been canceled by user
- `finished`- job has been finished successfully, also has `result` field
- `failed` - job has been finished unsuccessfully, also has `error` field


#### Websocket

For new UI there is websocket interface for listening lifecycle event from jobs
Events start sending to client after socket opening on one of available paths:
- `/v2/api/jobs/ws` - subscribe to events from all jobs
- `/v2/api/jobs/{id}/{ws}` - subscribe to events from particalur job



#### Async (MQTT, Kafka)

That types of interfaces are can listen livecycle events and send requests for starting new jobs.
Start request is json message and should has following format:
```js
{
  "endpointId": "endpointId",
  "parameters": {"foo": "bar"}, // job arguments
  "externalId": "myId" // optional
}

```
