[![Build Status](https://travis-ci.org/Hydrospheredata/mist.svg)](https://travis-ci.org/Hydrospheredata)
[![Coverage Status](https://coveralls.io/repos/github/Hydrospheredata/mist/badge.svg?branch=master)](https://coveralls.io/github/Hydrospheredata/mist?branch=master)
[![GitHub version](https://badge.fury.io/gh/hydrospheredata%2Fmist.svg)](https://badge.fury.io/gh/hydrospheredata%2Fmist) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.hydrosphere/mist_2.10/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.hydrosphere/mist_2.10/)
[![Dependency Status](https://www.versioneye.com/user/projects/5710b0cdfcd19a0045441000/badge.svg?style=flat)](https://www.versioneye.com/user/projects/5710b0cdfcd19a0045441000)
# Mist

Mist—is a thin service on top of Spark which makes it possible to execute Scala & Python Spark Jobs from application layers and get synchronous, asynchronous, and reactive results as well as provide an API to external clients.

It implements Spark as a Service and creates a unified API layer for building enterprise solutions and services on top of a Big Data lake.

![Mist use cases](http://hydrosphere.io/wp-content/uploads/2016/06/Mist-scheme-1050x576.png)

**Table of Contents**
- [Features](#features)
- [Version Information](#version-information)
- [Getting Started with Mist](#getting-started-with-mist)
- [Configuration](#configuration)
- [Logger](#logger)
- [Development mode](#development-mode)
- [Cluster mode](#cluster-mode)
- [Spark Job at Mist](#spark-job-at-mist)
- [Code Examples](#code-examples)
- [Tutorial](#tutorial)
- [API Reference](#api-reference)
- [Tests](#tests)
- [Contact](#contact)
- [License](#license)
- [Roadmap](#roadmap)

## Features

- HTTP and Messaging (MQTT) API
- Scala & Python Spark job execution
- Support for Spark SQL and Hive
- High Availability and Fault Tolerance
- Self Healing after driver program failure
- Super parallel mode: run Spark contexts in separate JVMs
- Powerful logging
- Maintence Spark 2.0.0

## Version Information

| Mist Version   | Scala Version  | Python Version | Spark Version    |
|----------------|----------------|----------------|------------------|
| 0.1.4          | 2.10.6         | 2.7.6          | >=1.5.2          |
| 0.2.0          | 2.10.6         | 2.7.6          | >=1.5.2          |
| 0.3.0          | 2.10.6         | 2.7.6          | >=1.5.2          |
| 0.4.0          | 2.10.6, 2.11.8 | 2.7.6          | >=1.5.2          |
| master         | 2.10.6, 2.11.8 | 2.7.6          | >=1.5.2          |


## Getting Started with Mist

######Dependencies
- jdk = 8
- scala = 2.10.6, (2.11.8 for Spark Version >=2.0.0)
- spark >= 1.5.2 (earlier versions were not tested)
- MQTT Server (optional)

######Running   
* Build the project

        git clone https://github.com/hydrospheredata/mist.git
        cd mist
        ./sbt/sbt -DsparkVersion=1.5.2 assembly # change version according to your installed spark
    
* Create [configuration file](#configuration)
* Run

        ./mist.sh   --config /path/to/application.conf \
                    --jar target/scala-2.10/mist-assembly-0.4.0.jar

##Configuration

Mist creates and orchestrates Apache Spark contexts automatically. All created contexts have their own name. Every job is run in a namespace. By default when you request a job to run the first time, Mist creates a namespace and new Spark context. The second request will use the created namespace so the context will be alive while Mist is running. This behavior can be changed in the configuration file: `mist.contextSetting.onstart` allows you to specify namespaces which must be run on start; `disposable` setting kills the context right after using it. You can set up options either for all contexts (`mist.contextDefault`) or for individual contexts (`mist.context.<namespace>`).


Configuration files are in [HOCON format](https://github.com/typesafehub/config/blob/master/HOCON.md)
```hocon
# spark master url can be either of three: local, yarn, mesos (local by default)
mist.spark.master = "local[*]"

# http interface (off by default)
mist.http.on = false
mist.http.host = "0.0.0.0"
mist.http.port = 2003

# MQTT interface (off by default)
mist.mqtt.on = false
mist.mqtt.host = "192.168.10.33"
mist.mqtt.port = 1883
# mist listens this topic for incoming requests
mist.mqtt.subscribe-topic = "foo"
# mist answers in this topic with the results
mist.mqtt.publish-topic = "foo"

# recovery job (off by default)
mist.recovery.on = false
# mist.recovery.multilimit = 10
# mist.recovery.typedb = "MapDb"
# mist.recovery.dbfilename = "file.db"

# default settings for all contexts
# timeout for each job in context
mist.context-defaults.timeout = 100 days
# mist can kill context after job finished (off by default)
mist.context-defaults.disposable = false

# settings for SparkConf
mist.context-defaults.spark-conf = {
    spark.default.parallelism = 128
    spark.driver.memory = "10g"
    spark.scheduler.mode = "FAIR"
}

# settings can be overridden for each context
mist.contexts.foo.timeout = 100 days

mist.contexts.foo.spark-conf = {
    spark.scheduler.mode = "FIFO"
}

mist.contexts.bar.timeout = 1000 second
mist.contexts.bar.disposable = true

# mist can create context on start, so we don't waste time on first request
mist.context-settings.onstart = ["foo"]

# mist log level
mist.akka {
  # Event handlers to register at boot time (Logging$DefaultLogger logs to STDOUT)
  # loggers = ["akka.event.Logging$DefaultLogger"]
  loggers = ["akka.event.slf4j.Slf4jLogger"]

  event-handlers = ["akka.event.slf4j.Slf4jEventHandler"]

  # Log level used by the configured loggers (see "event-handlers") as soon
  # as they have been started; before that, see "stdout-loglevel"
  # Options: OFF, ERROR, WARNING, INFO, DEBUG
  loglevel = "INFO"

  # logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"

  # Log level for the very basic logger activated during AkkaApplication startup
  # Options: OFF, ERROR, WARNING, INFO, DEBUG
  stdout-loglevel = "INFO"

  log-config-on-start = on
}
```

## Logger

You can use any logger on you project, for configure them, you should add appenders in logback.xml 

```
<configuration>

 <appender name="sout" class="ch.qos.logback.core.ConsoleAppender">
     <layout class="ch.qos.logback.classic.PatternLayout">
         <Pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</Pattern>
     </layout>
 </appender>

 <appender name="log4j" class="ch.qos.logback.core.FileAppender">
     <file>./logs/log_log4j.log</file>
     <layout class="ch.qos.logback.classic.PatternLayout">
         <Pattern>%date %level [%thread] %logger{10} [%file:%line] %msg%n</Pattern>
     </layout>
 </appender>

 <root level="INFO">
     <appender-ref ref="sout" />
     <appender-ref ref="log4j" />
 </root>

</configuration>
```

If you use log4j and SparkVersion erlier 2.0.0, you so will need create log4j.properties.
   
```
# Set everything to be logged to the console
log4j.rootCategory=DEBUG, file
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

log4j.appender.file=org.apache.log4j.FileAppender
log4j.appender.file.File=./logs/mist.log
log4j.appender.file.append=true
log4j.appender.file.layout=org.apache.log4j.PatternLayout
log4j.appender.file.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n

# Settings to quiet third party logs that are too verbose
log4j.logger.org.spark-project.jetty=WARN
log4j.logger.org.spark-project.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=INFO
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=INFO
log4j.logger.org.apache.parquet=ERROR
log4j.logger.parquet=ERROR

# SPARK-9183: Settings to avoid annoying messages when looking up nonexistent UDFs in SparkSQL with Hive support
log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler=FATAL
log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry=ERROR
```
 
 
## Spark Job at Mist

######Mist Scala Spark Job 

In order to prepare your job to run on Mist you should extend scala `object` from MistJob and implement abstract method *doStuff* :

```scala
def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = ???
def doStuff(context: SQLContext, parameters: Map[String, Any]): Map[String, Any] = ???
def doStuff(context: HiveContext, parameters: Map[String, Any]): Map[String, Any] = ???
```

for Version Spark >= 2.0.0

```scala
def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = ???
def doStuff(context: SparkSession, parameters: Map[String, Any]): Map[String, Any] = ???
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

######Building Mist jobs

Add Mist as dependency in your `build.sbt`:

```scala
libraryDependencies += "io.hydrosphere" % "mist" % "0.4.0"
```

Maven dependency:

```xml
<dependency>
    <groupId>io.hydrosphere</groupId>
    <artifactId>mist</artifactId>
    <version>0.4.0</version>
</dependency>
```
    
Link for direct download if you don't use a dependency manager:
* http://central.maven.org/maven2/io/hydrosphere/mist/

######Mist Python Spark Job 

Import [mist](https://github.com/Hydrospheredata/mist/tree/master/src/main/python) and implement method *doStuff*. 

The following are Spark Contexts aliases to be used for convenience:

```python
job.sc = SparkContext 
job.sqlc = SQL Context 
job.hc = Hive Context
```

for Version Spark >= 2.0.0

```python
job.sc = SparkContext 
job.ss = SparkSession
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
        
if __name__ == "__main__":
    job = MyJob(mist.Job())
```

## Development mode

You can use Vagrant and work in a preconfigured virtual machine.

```sh
git clone https://github.com/Hydrospheredata/mist
vagrant up
vagrant ssh
cd /vagrant
./sbt/sbt run
```

Use Vagrantfile to configure port forwarding and other network setup to make Mist available externally.

## Cluster mode

Mist could be deployed in [cluster mode](#cluster-mode) on Marathon with [Hydrosphere](http://hydrosphere.io/) [Springhead](https://github.com/provectus/springhead) for fault tolerance.

## Code Examples

* [Scala examples](https://github.com/Hydrospheredata/mist/tree/master/examples/src/main/scala)

* [Python examples](https://github.com/Hydrospheredata/mist/tree/master/examples/src/main/python)

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

## Tests

```sh
./sbt/sbt test
```

Test settings are in the file [reference.conf](https://github.com/Hydrospheredata/mist/tree/master/src/test/resources).


## Tutorial

Here we show how to get Mist running on localhost.  You will obviously need Spark installed.  In this example we use MQTT, but you could use HTTP instead by selecting that option in the Mist configuration file.

Install Moquito:

	sudo apt-get install mosquitto mosquitto-clients

Build Mist as explained above.

Create this configuration file mist.conf and save it someplace.  In this example we use /usr/src/mist/mist/mist.conf.


```hocon
mist.spark.master = "local[*]"

mist.settings.thread-number = 16

mist.http.on = true
mist.http.host = "127.0.0.1"
mist.http.port = 2003

mist.mqtt.on = false

mist.recovery.on = false

mist.contexts.foo.timeout = 100 days

mist.contexts.foo.spark-conf = {
  spark.default.parallelism = 4
  spark.driver.memory = "128m"
  spark.executor.memory = "64m"
  spark.scheduler.mode = "FAIR"
}

```

Next start Mist like this, changing the mist-assembly-X.X.X.jar file name to match the version you installed:

         ./mist.sh --config /usr/src/mist/mist/mist.conf --jar /usr/src/mist/mist/mistsrc/mist/target/scala-2.10/mist-assembly-0.4.0.jar
         
Set Python Path as shown below, again adjusting the file names and paths to match your installation:

        export PYTHONPATH=$PYTHONPATH:/usr/src/mist/mist/mistsrc/mist/src/main/python:$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.9-src.zip

Copy the code from above and save the sample Python code somewhere. The sample program iterates over and prints the parameters sent to it at runtime.
        
Run the sample using curl:

        curl --header "Content-Type: application/json" -X POST http://127.0.0.1:2003/jobs --data '{"pyPath":"/path to your file/Samplecode.py", "parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]}, "external_id":"12345678","name":"foo"}'

If everything is set up correctly it should say something like this message below, plus you will see messages in the Mist stdout console.

        {"success":true,"payload":{"result":[2,4,6,8,10,12,14,16,18,0]},"errors":[],"request":{"pyPath":"/home/walker/Documents/hydrosphere/mistExample.py","name":"foo","parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]},"external_id":"12345678"}}
        
Here is part of Mist stdout log where you can see that the program was submitted to Spark.  The output of the collect() statement will echo there as well.

        16/05/19 12:55:07 INFO SparkContext: Starting job: collect at /home/walker/Documents/hydrosphere/mistExample.py:16
        16/05/19 12:55:07 INFO DAGScheduler: Got job 0 (collect at /home/walker/Documents/hydrosphere/mistExample.py:16) with 4 output partitions
        16/05/19 12:55:07 INFO DAGScheduler: Final stage: ResultStage 0 (collect at /home/walker/Documents/hydrosphere/mistExample.py:16)
        16/05/19 12:55:07 INFO DAGScheduler: Parents of final stage: List()
        16/05/19 12:55:07 INFO DAGScheduler: Missing parents: List()
        16/05/19 12:55:07 INFO DAGScheduler: Submitting ResultStage 0 (PythonRDD[1] at collect at /home/walker/Documents/hydrosphere/mistExample.py:16), which has no missing parents


## Contact

Please report bugs/problems to: 
<https://github.com/Hydrospheredata/mist/issues>.

<http://hydrosphere.io/>

[LinkedIn](https://www.linkedin.com/company/hydrospherebigdata)

[Facebook](https://www.facebook.com/hydrosphere.io/)

[Twitter](https://twitter.com/hydrospheredata)

## License

Apache 2.0 License

## Roadmap

-----------------
- [x] Persist job state for self healing
- [x] Super parallel mode: run Spark contexts in separate JVMs
- [x] Powerful logging
- [ ] RESTification
- [ ] Cluster mode and node framework
- [ ] Support streaming contexts/jobs
- [ ] Apache Kafka support
- [ ] AMQP support
- [ ] Web Interface
