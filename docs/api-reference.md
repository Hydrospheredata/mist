## API Reference

Mist’s goal is to run Apache Spark jobs as a service. There might be fast (< 5s) and long running analytics jobs. Mist supports two modes: synchronous (HTTP) and asynchronous (MQTT). HTTP is straightforward: you make a POST request and then get results in a response. MQTT requests work almost the same: you send a message with a request into a specified topic ([configuration](#configuration): `mist.mqtt.subscribe-topic`) and then Mist sends a message back with the results ([configuration](#configuration): `mist.mqtt.publish-topic`). To dispatch multiple future responses you can add the `external_id` field into request message. `external_id` will be returned in a message with the results.

######Requests
for Scala jobs:

```javascript
{
    "jarPath": "/path/to/mist/job.jar",
    "className": "ExtendedMistJobObjectName",
    "parameters": { 
        /* optional paramateres, that will be available as "parameters" argument in "doStuff" method  */ 
    },
    "external_id": "", // optional field with any string inside
    "name": "foo" // mist context namespace
}
```
    
for Python jobs:

```javascript
{
    "pyPath": "/path/to/mist/job.py",
    "parameters": { 
        /* optional paramateres, that will be available as "parameters" argument in "doStuff" method  */ 
    },
    "external_id": "", // optional field with any string inside
    "name": "foo" // mist context namespace
}
```

e.g. from MQTT [(MQTT server and client)](http://mosquitto.org/) are necessary

```sh
mosquitto_pub -h 192.168.10.33 -p 1883 -m '{"jarPath":"/vagrant/examples/target/scala-2.10/mist_examples_2.10-0.4.0.jar", "className":"SimpleContext$","parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]}, "external_id":"12345678","name":"foo"}'  -t 'foo'
```

e.g. from HTTP

```sh
curl --header "Content-Type: application/json" -X POST http://192.168.10.33:2003/jobs --data '{"jarPath":"/vagrant/examples/target/scala-2.10/mist_examples_2.10-0.4.0.jar", "className":"SimpleContext$","parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]}, "external_id":"12345678","name":"foo"}'
```


######Response

```javascript
{
    "success": true,
    "payload":{ /* returned from doStuff value */ },
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
        "jarPath": "/vagrant/examples/target/scala-2.10/mist_examples_2.10-0.4.0.jar",
        "className": "SimpleContext$",
        "name": "foo",
        "parameters": {
            "digits": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]
        },
        "external_id":"12345678"
    }
}
```
