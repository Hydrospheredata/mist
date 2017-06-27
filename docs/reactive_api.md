### Reactive API

For reactive api mist supports MQTT and Kafka.
Via them you can run jobs and consume job-event from Mist.
For enable that kind of api you should set up them inside configuration file([example](https://github.com/Hydrospheredata/mist/blob/master/src/it/resources/mqtt/integration.conf#L27)).

Run-job request is json message and should has following format:
```js
{
  "endpointId": "endpointId",
  "parameters": {"foo": "bar"}, // job arguments
  "externalId": "myId" // optional
}
```
After that you can listen `publish-topic` to catch events from runned job

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
- `queued` - job has been queued (wait before worker assign job)
- `started` - job has been sended to worker
- `canceled` - job has been canceled by user
- `finished`- job has been finished successfully, also has `result` field
- `failed` - job has been finished unsuccessfully, also has `error` field

TODO: describe all events


#### Websocket

For new UI there is websocket interface for listening lifecycle event from jobs
Events start sending to client after socket opening on one of available paths:
- `/v2/api/jobs/ws` - subscribe to events from all jobs
- `/v2/api/jobs/{id}/ws` - subscribe to events from particular job
