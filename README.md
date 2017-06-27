[![Build Status](https://jenkins.hydrosphere.io/buildStatus/icon?job=hydrosphere/mist/master)](https://jenkins.hydrosphere.io/job/hydrosphere/job/mist/job/master/)
[![Build Status](https://travis-ci.org/Hydrospheredata/mist.svg)](https://travis-ci.org/Hydrospheredata)
[![GitHub version](https://badge.fury.io/gh/hydrospheredata%2Fmist.svg)](https://badge.fury.io/gh/hydrospheredata%2Fmist) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.hydrosphere/mist-lib-spark2_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.hydrosphere/mist-lib-spark2_2.11/)
[![Docker Hub Pulls](https://img.shields.io/docker/pulls/hydrosphere/mist.svg)](https://img.shields.io/docker/pulls/hydrosphere/mist.svg)
# Hydrosphere Mist

[Hydrosphere](http://hydrosphere.io) Mist is a service for exposing analytical jobs and machine learning models as web services.

Mist provides an API for Scala & Python Apache Spark jobs and for machine learning models trained in Apache Spark.

It implements Spark as a Service and creates a unified API layer for building enterprise solutions and services on top of a big data stack.

**[Getting Started Guide and documentation](/docs/README.md)**

------

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


## Contact

Please report bugs/problems to: 
<https://github.com/Hydrospheredata/mist/issues>.

<http://hydrosphere.io/>

[LinkedIn](https://www.linkedin.com/company/hydrospherebigdata)

[Facebook](https://www.facebook.com/hydrosphere.io/)

[Twitter](https://twitter.com/hydrospheredata)

