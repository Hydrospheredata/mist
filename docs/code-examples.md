## Code Examples

* [Scala examples](https://github.com/Hydrospheredata/mist/tree/master/examples/src/main/scala)

If you want to get examples.jar, you need clone mist from git and package jar file. _Note: mist docker container already contains built examples_

```sh
./sbt/sbt "project examples" package
```

this jar (`examples/target/scala-2.10/mist_examples_2.10-0.0.2.jar`), you can use from test Mist, for example, send Http requests

```
curl --header "Content-Type: application/json" -X POST http://mist_http_host:mist_http_port/jobs --data '{"path": "/path_to_jar/mist_examples.jar", "className": "SimpleContext$", "parameters": {"digits": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]}, "namespace": "foo"}'

curl --header "Content-Type: application/json" -X POST http://mist_http_host:mist_http_port/jobs --data '{"path": "/path_to_jar/mist_examples.jar", "className": "SimpleSQLContext$", "parameters": {"file": "/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "namespace": "foo"}'

curl --header "Content-Type: application/json" -X POST http://mist_http_host:mist_http_port/jobs --data '{"path": "/path_to_jar/mist_examples.jar", "className": "SimpleHiveContext$", "parameters": {"file": "/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "namespace": "foo"}'

```

or MQTT requests, for use MQTT you may need install MQTT client e.g. Mosquito

```
mosquitto_pub -h mist_mqtt_host -p mist_mqtt_port -m '{"path": "/path_to_jar/mist_examples.jar", "className": "SimpleContext$", "parameters": {"digits": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]}, "external_id": "12345678", "namespace": "foo"}'  -t 'foo'

mosquitto_pub -h mist_mqtt_host -p mist_mqtt_port -m '{"path": "/path_to_jar/mist_examples.jar", "className": "SimpleSQLContext$", "parameters": {"file": "/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "external_id": "12345678", "namespace": "foo"}'  -t 'foo'

mosquitto_pub -h mist_mqtt_host -p mist_mqtt_port -m '{"path": "/path_to_jar/mist_examples.jar", "className": "SimpleHiveContext$", "parameters":{"file": "/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "external_id": "12345678", "namespace": "foo"}'  -t 'foo'

```

* [Python examples](https://github.com/Hydrospheredata/mist/tree/master/examples/src/main/python)

all so you can use python examples, for this, you should export some env variables before starting Mist. _Note: mist docker container already includes set up environment_

```
export PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-*.zip:$PYTHONPATH
```

and send HTTP requests

``` 
curl --header "Content-Type: application/json" -X POST http://mist_http_host:mist_http_port/jobs --data '{"path": "/usr/share/mist/examples/src/main/python/simple_context.py", "className": "SimpleContext", "parameters": {"digits": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]}, "namespace": "foo"}'

curl --header "Content-Type: application/json" -X POST http://mist_http_host:mist_http_port/jobs --data '{"path": "/usr/share/mist/examples/src/main/python/simple_sql_context.py", "className": "SimpleSQLContext" "parameters": {"file": "/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "namespace": "foo"}'

curl --header "Content-Type: application/json" -X POST http://mist_http_host:mist_http_port/jobs --data '{"path": "/usr/share/mist/examples/src/main/python/simple_hive_context.py", "className": "SimpleHiveContext", "parameters": {"file": "/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "namespace": "foo"}'
```

or MQTT requests

```
mosquitto_pub -h mist_mqtt_host -p mist_mqtt_port -m '{"pyPath":"/usr/share/mist/examples/src/main/python/simple_context.py", "className": "SimpleContext", "parameters": {"digits": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]}, "external_id": "12345678", "namespace": "foo"}' -t 'foo'

mosquitto_pub -h mist_mqtt_host -p mist_mqtt_port -m '{"pyPath":"/usr/share/mist/examples/src/main/python/simple_sql_context.py", "className": "SimpleSQLContext", "parameters": {"file": "/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "external_id": "12345678", "namespace": "foo"}' -t 'foo'

mosquitto_pub -h mist_mqtt_host -p mist_mqtt_port -m '{"pyPath":"/usr/share/mist/examples/src/main/python/simple_sql_context.py", "className": "SimpleHiveContext", "parameters": {"file": "/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "external_id": "12345678", "namespace": "foo"}' -t 'foo'

```

for more example, you can go to [Mist Weather Example](https://github.com/Hydrospheredata/mist-weather-demo)



