[![Build Status](https://travis-ci.org/Hydrospheredata/mist.svg)](https://travis-ci.org/Hydrospheredata)
# Mist

Mist – a thin service on top of Spark which makes it possible to execute Scala & Python Spark Jobs from application layers and get synchronous, asynchronous, and reactive results as well as provide an API to external clients.

It implements a concept of Spark as a Service and creates a unified API layer for building entrprise solutions and services on a top of Big Data lake.

![Mist use cases](http://hydrosphere.io/wp-content/uploads/2016/03/scheme.png)

**Table of Contents**
- [Features](#features)
- [Version Information](#version-information)
- [Getting Started with Mist](#getting-started-with-mist)
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

- HTTP and Messaging (MQTT & AMPQ) API
- Scala & Python Spark Jobs execution
- Support for Spark SQL, Hive
- High Availability and Fault Tolerance

## Version Information

| Mist Version   | Scala Version  | Python Version | Spark Version  |
|----------------|----------------|----------------|----------------|
| 0.0.1          | 2.11.7         | 2.7.6          | 1.5.2          |
| master         | 2.11.7         | 2.7.6          | 1.5.2          |


## Getting Started with Mist

Build and run Mist in local [Development mode](#development-mode) with [SBT](http://www.scala-sbt.org/release/docs/Getting-Started/Setup.html)

Mist settings are in the file [reference.conf](https://github.com/Hydrospheredata/mist/tree/master/src/main/resources)

## Spark Job at Mist

######Mist Scala Spark Job 

In order to prepare your job to be run on Mist you should extend it from MistJob and implement abstract method *doStuff* :

    def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = ???
    def doStuff(context: SQLContext, parameters: Map[String, Any]): Map[String, Any] = ???
    def doStuff(context: HiveContext, parameters: Map[String, Any]): Map[String, Any] = ???

Example:

    object SimpleContext extends MistJob {
      override def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = {
        val numbers: List[BigInt] = parameters("digits").asInstanceOf[List[BigInt]]
        val rdd = context.parallelize(numbers)
        Map("result" -> rdd.map(x => x * 2).collect())
      }
    }
    
    
######Mist Python Spark Job 

Import [mist](https://github.com/Hydrospheredata/mist/tree/master/src/main/python) and implemented method *doStuff* 

There are following Spark Contexts aliases to be used for sonvenience:

```

 job.sc = SparkContext 
 job.sqlc = SQL Context 
 job.hc = Hive Context
 
```

for example:

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

## Development mode

You can use the Vagrant, and run preconfigured virtual machine

```
git clone https://github.com/Hydrospheredata/mist
vagrant up
ssh vagrant
cd /vagrant
./sbt/sbt run
```

Use Vagrantfile to configure port forwarding and other network setup and make Mist available externally.

## Cluster mode

Mist could be deployed in [Cluster mode](#cluster-mode) on Marathon with [hydrosphere](http://hydrosphere.io/) [springhead](https://github.com/provectus/springhead) for fault tolerance.

## Code Examples

* [Scala examples](https://github.com/Hydrospheredata/mist/tree/master/examples/src/main/scala)

* [Python examples](https://github.com/Hydrospheredata/mist/tree/master/examples/src/main/python)

## Configuration

<TBD> - need to descibe all the settings

## API Reference

To work with Mist, you can send messages on HTTP or MQTT 

The structure of the request must meet the following JSON

Job Requesting options

from jar:

```
  {
    "jarPath": {"type": "string"},
    "className": {"type": "string"},
    "parameters": {"type": "object"},
    "external_id": {"type": "string"},
    "name": {"type": "string"}
   }
  
```
    
from python:

```
  {
    "pyPath": {"type": "string"},
    "parameters": {"type": "object"},
    "external_id": {"type": "string"},
    "name": {"type": "string"}
   }
```

e.g. from MQTT [(Mosquitto)](http://mosquitto.org/)

    mosquitto_pub -h 192.168.10.33 -p 1883 -m '{"jarPath":"/vagrant/examples/target/scala-2.11/mist_examples_2.11-0.0.1.jar", "className":"SimpleContext$","parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]}, "external_id":"12345678","name":"foo"}'  -t 'foo'

e.g. from HTTP

    curl --header "Content-Type: application/json" -X POST http://192.168.10.33:2003/jobs --data '{"jarPath":"/vagrant/examples/target/scala-2.11/mist_examples_2.11-0.0.1.jar", "className":"SimpleContext$","parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]}, "external_id":"12345678","name":"foo"}'


Response scheme:

```
{
    "success": {"type": "boolean"},
    "payload":{"result": {"type": "object"}},
    "errors": {"type": "string"},
    "request": {"type": "string"}
}
```

e.g.

    {"success":true,"payload":{"result":[2,4,6,8,10,12,14,16,18,0]},"errors":[],"request":{"jarPath":"/vagrant/examples/target/scala-2.11/mist_examples_2.11-0.0.1.jar","className":"SimpleContext$","name":"foo","parameters":{"digits":[1,2,3,4,5,6,7,8,9,0]},"external_id":"12345678"}}


## Tests

```
git clone https://github.com/provectus/mist
```

`vagrant up`

`ssh vagrant`

`cd /vagrant`

`./sbt/sbt test`

Test settings are in the file [reference.conf](https://github.com/Hydrospheredata/mist/tree/master/src/test/resources)

## Contact

Please report bugs/problems to: 
<https://github.com/Hydrospheredata/mist/issues>

<http://hydrosphere.io/>

[LinkedIn](https://www.linkedin.com/company/hydrospherebigdata)

[Facebook](https://www.facebook.com/hydrosphere.io/)

[Twitter](https://twitter.com/hydrospheredata)

## License

## TODO

- Support Streaming Contexts/jobs
- Persist Job state for self healing
