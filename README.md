[![Build Status](https://jenkins.hydrosphere.io/buildStatus/icon?job=hydrosphere/mist/master)](https://jenkins.hydrosphere.io/job/hydrosphere/job/mist/job/master/)
[![Build Status](https://travis-ci.org/Hydrospheredata/mist.svg)](https://travis-ci.org/Hydrospheredata)
[![GitHub version](https://badge.fury.io/gh/hydrospheredata%2Fmist.svg)](https://badge.fury.io/gh/hydrospheredata%2Fmist) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.hydrosphere/mist-lib-spark2_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.hydrosphere/mist-lib-spark2_2.11/)
[![Docker Hub Pulls](https://img.shields.io/docker/pulls/hydrosphere/mist.svg)](https://img.shields.io/docker/pulls/hydrosphere/mist.svg)
# Hydrosphere Mist

[Hydrosphere](http://hydrosphere.io) Mist is a service for exposing analytical jobs and machine learning models as web services.

Mist provides an API for Scala & Python Apache Spark jobs and for machine learning models trained in Apache Spark.

It implements Spark as a Service and creates a unified API layer for building enterprise solutions and services on top of a big data stack.

![Mist use cases](http://hydrosphere.io/wp-content/uploads/2016/06/Mist-scheme-1050x576.png)

Discover more [Hydrosphere Mist use cases](/docs/use-cases/README.md).

**Table of Contents**
- [Features](#features)
- [Getting Started with Mist](#getting-started-with-mist)
- [Development mode](#development-mode)
- [Version Information](#version-information)
- [Roadmap](#roadmap)
- [Contact](#contact)
- [More docs](#more-docs)

## Features

- Realtime low latency models serving/scoring
![Mist Local Serving](http://dv9c7babquml0.cloudfront.net/docs-images/mist-model-serving.jpg)
- Spark Contexts orchestration - Cluster of Sark Clusters: manages multiple Spark contexts in separate JVMs or Dockers
![Cluster of Spark Clusters](http://dv9c7babquml0.cloudfront.net/docs-images/mist-cluster-of-spark-clusters.gif)
- Exposing Apache Spark jobs through REST API
- Spark **2.1.0** support! 
- HTTP & Messaging (MQTT, Kafka) API
- Scala and **Python** Spark jobs support
- Support for Spark SQL and Hive
- High Availability and Fault Tolerance
- Self Healing after driver program failure
- Powerful logging
- Clear end-user API

## Getting Started with Mist

##### Download locally

Dependencies:

- jdk = 8
- [spark](http://spark.apache.org/downloads.html) >= 1.5.2 (earlier versions were not tested)

- Download mist from <http://repo.hydrosphere.io/static/>
   ```sh
   wget http://repo.hydrosphere.io/static/mist-0.12.0-2.1.1.tar.gz
   tar xvfz mist-0.12.1-2.1.1.tar.gz
   cd mist-0.12.1-2.1.1
   SPARK_HOME=${path to spark distributive} bin/mist-master start --debug true
   ```
- Check how it works on http://localhost:2004/ui


##### Run from docker

- Run Docker:
    ```sh
    docker run -p 2004:2004\
        -v /var/run/docker.sock:/var/run/docker.sock
        -d hydrosphere/mist:0.12.0-2.1.1 mist
    ```
- Check how it works on http://localhost:2004/ui
        
[More about docker image](https://hub.docker.com/r/hydrosphere/mist/)
        
#### From sources

```sh
git clone git@github.com:Hydrospheredata/mist.git
cd mist
# for spark 2.1.0
sbt -DsparkVersion=2.1.0 mist/mistRun
# or use default
sbt mist/mistRun
```

##### Run example

```
curl --header "Content-Type: application/json" \
     -X POST http://localhost:2004/api/simple-context \
     --data '{"numbers": [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]}'
```

Check out [Complete Getting Started Guide](/docs/getting-started/README.md)

## What's next

* [Complete Getting Started Guide](/docs/getting-started/README.md)
* [Learn from Use Cases and Tutorials](/docs/use-cases/README.md)
    * [Enterprise Analytics Applications](/docs/use-cases/enterprise-analytics.md)
    * [Reactive Applications](/docs/use-cases/reactive.md)
    * [Realtime Machine Learning Applications](/docs/use-cases/ml-realtime.md)
* [Learn about Mist Routers](/docs/routes.md)
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
| 0.7.0          | 2.10.6, 2.11.8 | 2.7.6          | >=1.5.2          |
| 0.8.0          | 2.10.6, 2.11.8 | 2.7.6          | >=1.5.2          |
| 0.9.1          | 2.10.6, 2.11.8 | 2.7.6          | >=1.5.2          |
| 0.10.0         | 2.10.6, 2.11.8 | 2.7.6          | >=1.5.2          |
| master         | 2.10.6, 2.11.8 | 2.7.6          | >=1.5.2          |


## Roadmap

-----------------
- [x] Persist job state for self healing
- [x] Super parallel mode: run Spark contexts in separate JVMs
- [x] Powerful logging
- [x] RESTification
- [x] Support streaming contexts/jobs
- [x] Reactive API
- [x] Realtime ML models serving/scoring
- [x] CLI
- [x] Web Interface
- [x] Apache Kafka support
- [ ] Bi-directional streaming API
- [ ] AMQP support


## Docs Index

- [Getting Started](/docs/getting-started/README.md)
- [Use Cases & Tutorials](/docs/use-cases/README.md)
    - [Enterprise Analytics Applications](/docs/use-cases/enterprise-analytics.md)
    - [Reactive Applications](/docs/use-cases/reactive.md)
    - [Realtime Machine Learning Applications](/docs/use-cases/ml-realtime.md)
- [CLI](/docs/cli.md)
- [Scala & Python Mist DSL](/docs/spark-job-at-mist.md)
- [REST API](/docs/routes.md)
- [Streaming API](/docs/reactive.md)
- [Code Examples](/docs/code-examples.md)
- [Configuration](/docs/configuration.md)
- [License](/LICENSE)
- [Logging](/docs/logger.md)
- [Low level API Reference](/docs/api-reference.md)
- [Namespaces](/docs/context-namespaces.md)
- [Changelog](/CHANGELOG)
- [Tests](/docs/tests.md)

## Contact

Please report bugs/problems to: 
<https://github.com/Hydrospheredata/mist/issues>.

<http://hydrosphere.io/>

[LinkedIn](https://www.linkedin.com/company/hydrospherebigdata)

[Facebook](https://www.facebook.com/hydrosphere.io/)

[Twitter](https://twitter.com/hydrospheredata)

