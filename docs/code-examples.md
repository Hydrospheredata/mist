## Code Examples

* [Scala examples](https://github.com/Hydrospheredata/mist/tree/master/examples/src/main/scala)

If you want to get examples.jar, you need clone mist from git and run this commands

```
sbt
```

in SBT console you will need change project to examples

```
project examples
```

and package to jar

```
package
```

this jar, you can use from test Mist, for example, send Http requests

```
curl --header "Content-Type: application/json" -X POST http://mist_http_host:mist_http_port/jobs --data '{"jarPath":"/path_to_jar/mist_examples.jar", "className":"SimpleContext$","parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]}, "external_id":"12345678","name":"foo"}'

curl --header "Content-Type: application/json" -X POST http://mist_http_host:mist_http_port/jobs --data '{"jarPath":"/path_to_jar/mist_examples.jar","className":"SimpleSQLContext$","parameters":{"file":"/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "external_id":"12345678","name":"foo"}'

curl --header "Content-Type: application/json" -X POST http://mist_http_host:mist_http_port/jobs --data '{"jarPath":"/path_to_jar/mist_examples.jar","className":"SimpleHiveContext$","parameters":{"file":"/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "external_id":"12345678","name":"foo"}'

curl --header "Content-Type: application/json" -X POST http://mist_http_host:mist_http_port/jobs --data '{"jarPath":"/path_to_jar/mist_examples.jar","className":"SimpleHiveContext_SparkSession$","parameters":{"file":"/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "external_id":"12345678","name":"foo"}'

```

or MQTT requests, for use MQTT you may need install MQTT client e.g. Mosquito

```
mosquitto_pub -h mist_mqtt_host -p mist_mqtt_port -m '{"jarPath":"v/path_to_jar/mist_examples.jar", "className":"SimpleContext$","parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]}, "external_id":"12345678","name":"foo"}'  -t 'foo'

mosquitto_pub -h mist_mqtt_host -p mist_mqtt_port -m '{"jarPath":"/path_to_jar/mist_examples.jar","className":"SimpleSQLContext$","parameters":{"file":"/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "external_id":"12345678","name":"foo"}'  -t 'foo'

mosquitto_pub -h mist_mqtt_host -p mist_mqtt_port -m '{"jarPath":"/path_to_jar/mist_examples.jar","className":"SimpleHiveContext$","parameters":{"file":"/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "external_id":"12345678","name":"foo"}'  -t 'foo'

mosquitto_pub -h mist_mqtt_host -p mist_mqtt_port -m '{"jarPath":"/path_to_jar/mist_examples.jar","className":"SimpleHiveContext_SparkSession$","parameters":{"file":"/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "external_id":"12345678","name":"foo"}'  -t 'foo'

```

* [Python examples](https://github.com/Hydrospheredata/mist/tree/master/examples/src/main/python)

all so you can use python examples, for this, you should add exports

```
export PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-*.zip:$PYTHONPATH
export PYTHONPATH=$PYTHONPATH:/pathToMist/src/main/python/
export PYTHONPATH=$PYTHONPATH:src/main/python/
```

and send HTTP requests

```
curl --header "Content-Type: application/json" -X POST http://mist_http_host:mist_http_port/jobs --data '{"pyPath":"/vagrant/examples/src/main/python/example.py", "parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]}, "external_id":"12345678","name":"foo"}'

curl --header "Content-Type: application/json" -X POST http://mist_http_host:mist_http_port/jobs --data '{"pyPath":"/vagrant/examples/src/main/python/exampleSQL.py", "parameters":{"file":"/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "external_id":"12345678","name":"foo"}'

curl --header "Content-Type: application/json" -X POST http://mist_http_host:mist_http_port/jobs --data '{"pyPath":"/vagrant/examples/src/main/python/exampleHIVE.py", "parameters":{"file":"/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "external_id":"12345678","name":"foo"}'
```

or MQTT requests

```
mosquitto_pub -h mist_mqtt_host -p mist_mqtt_port -m '{"pyPath":"/vagrant/examples/src/main/python/example.py", "parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]}, "external_id":"12345678","name":"foo"}' -t 'foo'

mosquitto_pub -h mist_mqtt_host -p mist_mqtt_port -m '{"pyPath":"/vagrant/examples/src/main/python/exampleSQL.py", "parameters":{"file":"/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "external_id":"12345678","name":"foo"}' -t 'foo'

mosquitto_pub -h mist_mqtt_host -p mist_mqtt_port -m '{"pyPath":"/vagrant/examples/src/main/python/exampleHIVE.py", "parameters":{"file":"/path_to_mist/examples/resources/SimpleSQLContextData.json"}, "external_id":"12345678","name":"foo"}' -t 'foo'

```

for more example, you can go to [Mist Weather Example](https://github.com/Hydrospheredata/mist-weather-demo)



