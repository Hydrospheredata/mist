## Code Examples

* [Scala examples for spark1](https://github.com/Hydrospheredata/mist/tree/master/examples-spark1)
* [Scala examples for spark2](https://github.com/Hydrospheredata/mist/tree/master/examples-spark2)

Package examples

```sh
./sbt/sbt "project examples-spark2" package
./sbt/sbt "project examples-spark1" package
```

_Note: Mist docker container already includes pre-built examples_

Start Mist and test HTTP requests:

```
curl --header "Content-Type: application/json" -X POST http://mist_http_host:mist_http_port/api/simple-context --data '{"numbers": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]}'

```

Then install MQTT client, e.g. Mosquito, and test MQTT endpoint:

```
mosquitto_pub -h mist_mqtt_host -p mist_mqtt_port -m '{"jobId": "simple-context", "action": "execute", "parameters": {"numbers": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]}, "externalId": "12345678"}'  -t 'foo'
```

* [Python examples](https://github.com/Hydrospheredata/mist/tree/master/examples-python)

Export env variables: 

```
export PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-*.zip:$PYTHONPATH
```
_Note: mist docker container already includes set up environment_

Then start Mist and test HTTP requests:

``` 
curl --header "Content-Type: application/json" -X POST http://mist_http_host:mist_http_port/api/simple-context-py --data '{"numbers": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]}'
```

Then test MQTT requests:

```
mosquitto_pub -h mist_mqtt_host -p mist_mqtt_port -m '{"jobId": "simple-context-py", "action": "execute", "parameters": {"numbers": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]}, "externalId": "12345678"}'  -t 'foo'
```

To explore end-to-end Apache Spark, Mist and Node.js reference project check out [Demo](https://github.com/mkf-simpson/crimethory) repository.



