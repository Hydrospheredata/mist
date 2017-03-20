## Low level API Reference

It is recommented to use [Routes abstraction](routes.md) to expose Hydrosphere Mist jobs as a REST endpoints.

Howewer it is possible to use low level API described below.

Mist’s goal is to run Apache Spark jobs as a service. There might be fast (< 5s) and long running analytics jobs. Mist supports two modes: synchronous (HTTP) and asynchronous (MQTT). HTTP is straightforward: you make a POST request and then get results in a response. MQTT requests work almost the same: you send a message with a request into a specified topic ([configuration](configuration.md): `mist.mqtt.subscribe-topic`) and then Mist sends a message back with the results ([configuration](configuration.md): `mist.mqtt.publish-topic`).

######Requests

Any job can be run via HTTP or async (MQTT) interface and besides requests for both ones are identically. 

```javascript
{
    "path": "/path/to/mist/job.jar",
    "className": "ExtendedMistJobObjectName",
    "parameters": { 
        /* optional paramateres, that will be available as arguments in "execute" method  */ 
    },
    "externalId": "", // optional field with any string inside
    "namespace": "foo" // mist context namespace
}
```

* `path`: Request must contain `path` field with path to Mist job: .jar or .py file (can be local or in hdfs). 
* `className`: Since .jar and .py files can include multiple classes it's necessary to specify `className` parameter. _Note: you should add `$` sign to the end of scala `object` name._ 
* `namespace`: Every Mist job could be run in it's own Apache Spark context or share that context with other jobs. It is possible to specify [context namespace](context-namespaces.md) in low level API request.
* `parameters`: `execute` method always takes `parameters: Map[String, Any]` argument. This argument is json subdocument in `parameters` field and could have job specific parameters to be specified by API user.
* `externalId`: `externalId` is used to dispatch multiple future responses from async calls. `externalId` will be returned beck in a response message.

MQTT example ([MQTT server and client](http://mosquitto.org/) are required):

```sh
mosquitto_pub -h 192.168.10.33 -p 1883 -m '{"path": "./examples/target/scala-2.10/mist_examples_2.10-0.10.0.jar", "className": "SimpleContext$","parameters":{"numbers":[1,2,3,4,5,6,7,8,9,0]}, "externalId": "12345678", "namespace": "foo"}'  -t 'foo'
```

HTTP example:

```sh
curl --header "Content-Type: application/json" -X POST http://192.168.10.33:2003/jobs --data '{"path": "./examples/target/scala-2.10/mist_examples_2.10-0.10.0.jar", "className": "SimpleContext$", "parameters": {"numbers": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]}, "externalId": "12345678", "namespace": "foo"}'
```


######Response

```javascript
{
    "success": true,
    "payload":{ /* returned from execute method value */ },
    "errors": [ /* array of string with errors */ ],
    "request": { /* clone of request */ }
}
```

e.g.
```javascript
{
    "success": true,
    "payload": {
        "result": [2, 4, 6, 8, 10, 12, 14, 16, 18, 0]
    },
    "errors": [],
    "request": {
        "jarPath": "/vagrant/examples/target/scala-2.10/mist_examples_2.10-0.10.0.jar",
        "className": "SimpleContext$",
        "namespace": "foo",
        "parameters": {
            "numbers": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]
        },
        "externalId":"12345678"
    }
}
```
