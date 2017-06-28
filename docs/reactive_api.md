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

TODO: describe all events


#### Websocket

For new UI there is a websocket interface for listening to lifecycle events from jobs
Events are sent to the client to the following paths:
- `/v2/api/jobs/ws` - subscribe to events from all jobs
- `/v2/api/jobs/{id}/ws` - subscribe to events from particular job

### Next 
- [CLI](/docs/cli.md)
- [Configuration](/docs/configuration.md)
- [Contexts](/docs/context-namespaces.md)