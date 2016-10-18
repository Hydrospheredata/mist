## Mist RESTificated Routes

By default Routes are described in `./configs/router.conf`. See [configuration docs](configuration.md) for more details. For example:

```hocon
forecast = {
    path = 'hdfs://hdfs-host/jobs/forecast-job.jar',
    className = 'FastForecast$',
    name = 'production-namespace'
}
```

This route describes REST API endpoint `/api/forecast`, so users could send simple REST request and get result back:

```javascript
POST /api/forecast
{
    "param": 1
}
```

The same route could be used in asynchrounous mode, e.g. MQTT message will look like

```javascript
{
    "route": "forecast",
    "parameters": {
      "param": 1
    }
}
```
