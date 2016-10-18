## Mist RESTificated Routes

Default Mist requests contain [a lot of internal information](api-reference.md) such as path to job file, class name or namespace. For reasons of clearness these things can be hidden for API users with configuring routes.

Routes configuration is a separate file (`./configs/router.conf` by default, see [configuration docs](configuration.md) for details) which includes API route and its settings. For example:

```hocon
forecast = {
    path = 'hdfs://hdfs-host/jobs/forecast-job.jar',
    className = 'FastForecast$',
    name = 'production-namespace'
}
```

So instead of large incomprehensible request on `/job` endpoint

```javascript
{
    "path": "hdfs://hdfs-host/jobs/forecast-job.jar",
    "className": "FastForecast$",
    "name": "production-namespace",
    "paramters": {
        "param": 1
    }
}
```

users can send simple REST-like one on `/api/forecast` endpoint:

```javascript
{
    "param": 1
}
```
