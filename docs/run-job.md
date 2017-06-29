## Run Mist job

### Configuration

Endpoint is an abstraction over jobs.
Every endpoints is combination of `artifactFile`, `className` of class that implements MistJob and default `namespace`.
Endpoints are configured in `./configs/router.conf` or can be passed to mist-master using `--router-conf` argument
It allows building user friendly endpoints by exposing only domain specific parameters. It semantically decouples job internals and business logic.

Example configuration:

```hocon
my-awesome-job { // object name is name of endpoint
    path = "my-awesome-job.jar" // path to artifact file (jar or py). Could be local, HDFS or Maven
    className = "com.company.MyAwesomeJob$" // full class name ob job object
    namespace = "production-namespace"
}
```

### Run

There is several ways to run job on endpoint:

- Http api:
   - Run job - obtain id assciated with that run request:
    ```sh
    curl --header "Content-Type: application/json" \
         -X POST http://localhost:2004/v2/api/endpoints/my-awesome-job/jobs?force=true \
         --data '{"arg": "value"}'
    ```
- Send message over Kafka or Mqtt with format:
   ```javascript
    {
      "endpointId": "my-awesome-job",
      "parameters": {
        "arg1": "value"
      }
    }
   ```
### Next 
- [Http API](/docs/http_api.md)
- [Reactive API](/docs/reactive_api.md)
- [CLI](/docs/cli.md)
