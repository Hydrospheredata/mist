### Reactive API

For reactive api mist supports MQTT and Kafka.
You can run jobs and consume job-event from Mist.
By default it is turned off. Check out [configs example](https://github.com/Hydrospheredata/mist/blob/master/src/it/resources/mqtt/integration.conf#L27) to see how to turn it on.

Run-job request is json message and should has following format:
```js
{
  "endpointId": "endpointId",
  "parameters": {"foo": "bar"}, // job arguments
  "externalId": "myId" // optional
  "runSettings": {
      "contextId": "default" // optional should be used to override endpoint context
      "workerId": "worker-postfix" // optional should be used to identify worker by this postfix
  } // optional
}
```
Then you can listen `publish-topic` to catch events from the job

#### Livecycle events

There were several async interfaces described bellow, that can be used to track lifecycle events from jobs.
All events have 2 required fields: `event`, `id`, and some of them provide more:
```js
{
  "event": "event-name",
  "id": "job id"
}
```

- `initialized` - job has been assigned to invocation, also returns all job parameters
- `queued` - job has been queued (wait before worker assign job)
- `started` - job has been sent to worker
- `canceled` - job has been canceled by user
- `finished`- job has been finished successfully, also has `result` field
- `failed` - job has been finished unsuccessfully, also has `error` field
-  `logs` - [struct](https://github.com/Hydrospheredata/mist/blob/4bd40459e4c6780d2b3e7d6fe6e6e1c9f5c14174/src/main/scala/io/hydrosphere/mist/Messages.scala#L98)

TODO: describe all events


#### Websocket

For new UI there is a websocket interface for listening to lifecycle events from jobs
Events are sent to the client to the following paths:
- `/v2/api/ws` - subscribe to all events from mist
- `/v2/api/jobs/{id}/ws` - subscribe to events from particular job

### Next 
- [CLI](/docs/cli.md)
- [Configuration](/docs/configuration.md)
- [Contexts](/docs/context-namespaces.md)
