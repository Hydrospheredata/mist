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
- [Getting Started with Mist](#getting-started-with-mist)
- [Development mode](#development-mode)
- [Version Information](#version-information)
- [Roadmap](#roadmap)
- [Contact](#contact)
- [More docs](#more-docs)

## Features

- Spark **2.0.0** support!
- Spark Contexts orchestration
- Super parallel mode: multiple Spark contexts in separate JVMs
- HTTP & Messaging (MQTT) API
- Scala & **Python** Spark jobs support
- Support for Spark SQL and Hive
- High Availability and Fault Tolerance
- Self Healing after driver program failure
- Powerful logging
- Clear end-user API

## Getting Started with Mist

######Dependencies
- jdk = 8
- spark >= 1.5.2 (earlier versions were not tested)
- MQTT Server (optional)

######Run mist   

        docker run -p 2003:2003 -v /var/run/docker.sock:/var/run/docker.sock -d hydrosphere/mist:master-2.0.0 mist
        
[More about docker image](https://hub.docker.com/r/hydrosphere/mist/)
        
######Run example

```
sbt "project examples" package

curl --header "Content-Type: application/json" -X POST http://localhost:2003/api/simple-context --data '{"digits": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]}'
```

[Complete Getting Started Guide](/docs/getting-started/README.md)

## Building from source

* Build the project

        git clone https://github.com/hydrospheredata/mist.git
        cd mist
        sbt -DsparkVersion=2.0.0 assembly 
    
* Run

        ./bin/mist start master

## Development mode

```sh
# clone mist repo 
git clone https://github.com/Hydrospheredata/mist

# available spark versions: 1.5.2, 1.6.2, 2.0.0
export SPARK_VERSION=2.0.0
docker create --name mist-${SPARK_VERSION} -v /usr/share/mist hydrosphere/mist:tests-${SPARK_VERSION}
docker run --name mosquitto-${SPARK_VERSION} -d ansi/mosquitto
docker run --name hdfs-${SPARK_VERSION} --volumes-from mist-${SPARK_VERSION} -d hydrosphere/hdfs start

# run tests
docker run -v /var/run/docker.sock:/var/run/docker.sock --link mosquitto-${SPARK_VERSION}:mosquitto --link hdfs-${SPARK_VERSION}:hdfs -v $PWD:/usr/share/mist hydrosphere/mist:tests-${SPARK_VERSION} tests
# or run mist
docker run -v /var/run/docker.sock:/var/run/docker.sock --link mosquitto-${SPARK_VERSION}:mosquitto --link hdfs-${SPARK_VERSION}:hdfs -v $PWD:/usr/share/mist hydrosphere/mist:tests-${SPARK_VERSION} mist
```

## What's next

* [Write and build your own Mist job](/docs/spark-job-at-mist.md)
* [Make awesome API for it](/docs/routes.md)
* [Configure mist to make it fast and reliable](/docs/configuration.md)

## Version Information

| Mist Version   | Scala Version  | Python Version | Spark Version    |
|----------------|----------------|----------------|------------------|
| 0.1.4          | 2.10.6         | 2.7.6          | >=1.5.2          |
| 0.2.0          | 2.10.6         | 2.7.6          | >=1.5.2          |
| 0.3.0          | 2.10.6         | 2.7.6          | >=1.5.2          |
| 0.4.0          | 2.10.6, 2.11.8 | 2.7.6          | >=1.5.2          |
| 0.5.0          | 2.10.6, 2.11.8 | 2.7.6          | >=1.5.2          |
| 0.6.5          | 2.10.6, 2.11.8 | 2.7.6          | >=1.5.2          |
| master         | 2.10.6, 2.11.8 | 2.7.6          | >=1.5.2          |


## Roadmap

-----------------
- [x] Persist job state for self healing
- [x] Super parallel mode: run Spark contexts in separate JVMs
- [x] Powerful logging
- [x] RESTification
- [x] Support streaming contexts/jobs
- [x] Reactive API
- [ ] Cluster mode and Mesos node framework
- [ ] Apache Kafka support
- [ ] AMQP support
- [ ] Web Interface


## More docs

- [Getting Started](/docs/getting-started/README.md)
- [Use Cases & Tutorials](/docs/use-cases/README.md)
    - [Enterprise Analytics Applications](/docs/use-cases/enterprise-analytics.md)
    - [Reactive Applications](/docs/use-cases/reactive.md)
    - [Realtime Machine Learning Applications](/docs/use-cases/ml-realtime.md)
- [Scala & Python Mist DSL](/docs/spark-job-at-mist.md)
- [REST API](/docs/routes.md)
- [Streaming API](/docs/reactive.md)
- [Code Examples](/docs/code-examples.md)
- [Configuration](/docs/configuration.md)
- [Low level API Reference](/docs/api-reference.md)
- [Namespaces](/docs/context-namespaces.md)
- [Logging](/docs/logger.md)
- [Tests](/docs/tests.md)
- [License](/LICENSE)
- [Changelog](/CHANGELOG)

## Contact

Please report bugs/problems to: 
<https://github.com/Hydrospheredata/mist/issues>.

<http://hydrosphere.io/>

[LinkedIn](https://www.linkedin.com/company/hydrospherebigdata)

[Facebook](https://www.facebook.com/hydrosphere.io/)

[Twitter](https://twitter.com/hydrospheredata)

