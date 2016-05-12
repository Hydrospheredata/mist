[![Build Status](https://travis-ci.org/Hydrospheredata/mist.svg)](https://travis-ci.org/Hydrospheredata)
[![Coverage Status](https://coveralls.io/repos/github/Hydrospheredata/mist/badge.svg?branch=master)](https://coveralls.io/github/Hydrospheredata/mist?branch=master)
[![GitHub version](https://badge.fury.io/gh/hydrospheredata%2Fmist.svg)](https://badge.fury.io/gh/hydrospheredata%2Fmist) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.hydrosphere/mist_2.10/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.hydrosphere/mist_2.10/)
[![Dependency Status](https://www.versioneye.com/user/projects/5710b0cdfcd19a0045441000/badge.svg?style=flat)](https://www.versioneye.com/user/projects/5710b0cdfcd19a0045441000)
# Mist

Mist—is a thin service on top of Spark which makes it possible to execute Scala & Python Spark Jobs from application layers and get synchronous, asynchronous, and reactive results as well as provide an API to external clients.

It implements Spark as a Service and creates a unified API layer for building enterprise solutions and services on top of a Big Data lake.

![Mist use cases](http://hydrosphere.io/wp-content/uploads/2016/03/scheme.png)

**Table of Contents**
- [Features](#features)
- [Version Information](#version-information)
- [Getting Started with Mist](#getting-started-with-mist)
- [Configuration](#configuration)
- [Development mode](#development-mode)
- [Cluster mode](#cluster-mode)
- [Spark Job at Mist](#spark-job-at-mist)
- [Code Examples](#code-examples)
- [API Reference](#api-reference)
- [Tests](#tests)
- [Contact](#contact)
- [License](#license)
- [TODO](#todo)

## Features

- HTTP and Messaging (MQTT) API
- Scala & Python Spark job execution
- Support for Spark SQL and Hive
- High Availability and Fault Tolerance

## Version Information

| Mist Version   | Scala Version  | Python Version | Spark Version    |
|----------------|----------------|----------------|------------------|
| 0.1.0          | 2.10.6         | 2.7.6          | >=1.5.2          |
| master         | 2.10.6         | 2.7.6          | >=1.5.2          |


## Getting Started with Mist

######Dependencies
- jdk = 8
- scala = 2.10.6
- spark >= 1.5.2 (earlier versions were not tested)
- MQTT Server *(optional)*

######Running
* Build the project

        git clone https://github.com/hydrospheredata/mist.git
        cd mist
        ./sbt/sbt -DsparkVersion=1.5.2 assembly # change version according to your installed spark
    
* Create [configuration file](#configuration)
* Run

        java -Dconfig.file=/path/to/application.conf -jar target/scala-2.10/mist-assembly-0.1.0.jar

##Configuration

Mist creates and orchestrates Apache Spark contexts automatically. All created contexts have their own name. Every job is run in a namespace. By default when you request a job to run the first time, mist creates a namespace and new Spark context. The second request will use the created namespace so the context will be alive while Mist is running. This behavior can be changed in the configuration file: `mist.contextSetting.onstart` allows you to specify namespaces which must be run on start; `disposable` setting kills the context right after using it. You can set up options either for all contexts (`mist.contextDefault`) or for individual contexts (`mist.context.<namespace>`).


Configuration files are in [HOCON format](https://github.com/typesafehub/config/blob/master/HOCON.md)
```hocon
# spark master url can be either of three: local, yarn, mesos (local by default)
mist.spark.master = "local[*]"

# number of threads: one thread for one job
mist.settings.threadNumber = 16

# http interface (off by default)
mist.http.on = false
mist.http.host = "0.0.0.0"
mist.http.port = 2003

# MQTT interface (off by default)
mist.mqtt.on = false
mist.mqtt.host = "192.168.10.33"
mist.mqtt.port = 1883
# mist listens this topic for incoming requests
mist.mqtt.subscribeTopic = "foo"
# mist answers in this topic with the results
mist.mqtt.publishTopic = "foo"

# default settings for all contexts
# timeout for each job in context
mist.contextDefaults.timeout = 100 days
# mist can kill context after job finished (off by default)
mist.contextDefaults.disposable = false

# settings for SparkConf
mist.contextDefaults.sparkConf = {
    spark.default.parallelism = 128
    spark.driver.memory = "10g"
    spark.scheduler.mode = "FAIR"
}

# settings can be overridden for each context
mist.contexts.foo.timeout = 100 days

mist.contexts.foo.sparkConf = {
    spark.scheduler.mode = "FIFO"
}

mist.contexts.bar.timeout = 1000 second
mist.contexts.bar.disposable = true

# mist can create context on start, so we don't waste time on first request
mist.contextSettings.onstart = ["foo"]
```

## Spark Job at Mist

######Mist Scala Spark Job 

In order to prepare your job to run on Mist you should extend scala `object` from MistJob and implement abstract method *doStuff* :

```scala
def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = ???
def doStuff(context: SQLContext, parameters: Map[String, Any]): Map[String, Any] = ???
def doStuff(context: HiveContext, parameters: Map[String, Any]): Map[String, Any] = ???
```

Example:

```scala
object SimpleContext extends MistJob {
    override def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = {
        val numbers: List[BigInt] = parameters("digits").asInstanceOf[List[BigInt]]
        val rdd = context.parallelize(numbers)
        Map("result" -> rdd.map(x => x * 2).collect())
    }
}
```

######Building mist jobs

Add mist as dependency in your `build.sbt`:

```scala
libraryDependencies += "io.hydrosphere" % "mist" % "0.1.0"
```

Maven dependency:

```xml
<dependency>
    <groupId>io.hydrosphere</groupId>
    <artifactId>mist</artifactId>
    <version>0.1.0</version>
</dependency>
```
    
Link for direct download if you don't use a dependency manager:
* http://central.maven.org/maven2/io/hydrosphere/mist/

######Mist Python Spark Job 

Import [mist](https://github.com/Hydrospheredata/mist/tree/master/src/main/python) and implemented method *doStuff*. 

The following are Spark Contexts aliases to be used for convenience:

```python
job.sc = SparkContext 
job.sqlc = SQL Context 
job.hc = Hive Context
```

for example:
```python
import mist
class MyJob:
    def __init__(self, job):
        job.sendResult(self.doStuff(job))
    def doStuff(self, job):
        val = job.parameters.values()
        list = val.head()
        size = list.size()
        pylist = []
        count = 0
        while count < size:
            pylist.append(list.head())
            count = count + 1
            list = list.tail()
        rdd = job.sc.parallelize(pylist)
        result = rdd.map(lambda s: 2 * s).collect()
        return result
job = MyJob(mist.Job())
```

## Development mode

You can use Vagrant and work in a preconfigured virtual machine

```sh
git clone https://github.com/Hydrospheredata/mist
vagrant up
vagrant ssh
cd /vagrant
./sbt/sbt run
```

Use Vagrantfile to configure port forwarding and other network setup to make Mist available externally.

## Cluster mode

Mist could be deployed in [Cluster mode](#cluster-mode) on Marathon with [hydrosphere](http://hydrosphere.io/) [springhead](https://github.com/provectus/springhead) for fault tolerance.

## Code Examples

* [Scala examples](https://github.com/Hydrospheredata/mist/tree/master/examples/src/main/scala)

* [Python examples](https://github.com/Hydrospheredata/mist/tree/master/examples/src/main/python)

## API Reference

Mist’s goal is to run Apache Spark jobs as a service. There might be fast (< 5s) and long running analytics jobs. Mist supports two modes: synchronous (HTTP) and asynchronous (MQTT). HTTP is straightforward: you make a POST request and then get results in a response. MQTT requests work almost the same: you send a message with a request into a specified topic ([configuration](#configuration): `mist.mqtt.subscribeTopic`) and then mist sends a message back with the results ([configuration](#configuration): `mist.mqtt.publishTopic`). To dispatch multiple future responses you can add the `external_id` field into request message. `external_id` will be returned in a message with the results.

######Requests
for scala jobs:

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
    
for python jobs:

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
mosquitto_pub -h 192.168.10.33 -p 1883 -m '{"jarPath":"/vagrant/examples/target/scala-2.10/mist_examples_2.10-0.1.0.jar", "className":"SimpleContext$","parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]}, "external_id":"12345678","name":"foo"}'  -t 'foo'
```

e.g. from HTTP

```sh
curl --header "Content-Type: application/json" -X POST http://192.168.10.33:2003/jobs --data '{"jarPath":"/vagrant/examples/target/scala-2.10/mist_examples_2.10-0.1.0.jar", "className":"SimpleContext$","parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]}, "external_id":"12345678","name":"foo"}'
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
        "jarPath": "/vagrant/examples/target/scala-2.10/mist_examples_2.10-0.1.0.jar",
        "className": "SimpleContext$",
        "name": "foo",
        "parameters": {
            "digits": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]
        },
        "external_id":"12345678"
    }
}
```

## Tests

```sh
./sbt/sbt test
```

Test settings are in the file [reference.conf](https://github.com/Hydrospheredata/mist/tree/master/src/test/resources)

## Contact

Please report bugs/problems to: 
<https://github.com/Hydrospheredata/mist/issues>

<http://hydrosphere.io/>

[LinkedIn](https://www.linkedin.com/company/hydrospherebigdata)

[Facebook](https://www.facebook.com/hydrosphere.io/)

[Twitter](https://twitter.com/hydrospheredata)

## License

Apache 2.0 License

## TODO

- Support Streaming Contexts/jobs
- Persist Job state for self healing
- Super parallel mode
- Apache Kafka support
- AMQP support
