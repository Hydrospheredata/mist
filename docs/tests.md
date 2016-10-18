## Tests

The easiest way to run tests is run them inside docker container.

First of all export SPARK_VERSION with version you want to work: 
```sh
export $SPARK_VERSION=2.0.0
```

Then create container with testing source code (inside work directory):
```sh
docker create --name mist-${SPARK_VERSION} -v /usr/share/mist hydrosphere/mist:tests-${SPARK_VERSION}
```

You need MQTT server and HDFS to run all Mist tests. Everything you can run with preconfigured docker containers:

```sh
docker run --name mosquitto-${SPARK_VERSION} -d ansi/mosquitto
docker run --name hdfs-${SPARK_VERSION} --volumes-from mist-${SPARK_VERSION} -d hydrosphere/hdfs start
```

Now you can run scala tests:

```sh
docker run --link mosquitto-${SPARK_VERSION}:mosquitto --link hdfs-${SPARK_VERSION}:hdfs -v $PWD:/usr/share/mist hydrosphere/mist:tests-${SPARK_VERSION} tests
```

Test settings are in the file [reference.conf](https://github.com/Hydrospheredata/mist/tree/master/src/test/resources).


