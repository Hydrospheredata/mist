[![Build Status](https://jenkins.hydrosphere.io/buildStatus/icon?job=hydrosphere/mist/master)](https://jenkins.hydrosphere.io/job/hydrosphere/job/mist/job/master/)
[![Build Status](https://travis-ci.org/Hydrospheredata/mist.svg?branch=master)](https://travis-ci.org/Hydrospheredata)
[![GitHub version](https://badge.fury.io/gh/hydrospheredata%2Fmist.svg)](https://badge.fury.io/gh/hydrospheredata%2Fmist) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.hydrosphere/mist-lib-spark2_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.hydrosphere/mist-lib-spark2_2.11/)
[![Docker Hub Pulls](https://img.shields.io/docker/pulls/hydrosphere/mist.svg)](https://img.shields.io/docker/pulls/hydrosphere/mist.svg)
# Hydrosphere Mist

[Hydrosphere](http://hydrosphere.io) Mist is a Multi-tenancy and Multi-user Spark server.

Main features:
* **Serverless**. Get abstracted from resource isolation, sharing and auto-scaling. 
* **REST** HTTP & Messaging (MQTT, Kafka) API for Scala & Python Spark jobs.
* Compatibility with EMR, Hortonworks, Cloudera, DC/OS and vanilla Spark distributions.
* Spark **MLLib serving** that has been moved to [spark-ml-serving](https://github.com/Hydrospheredata/spark-ml-serving) library and [hydro-serving](https://github.com/Hydrospheredata/hydro-serving) project

It implements Spark Compute as a Service and creates a unified API layer for building enterprise solutions and services on top of a big data stack.

![Mist use cases](http://hydrosphere.io/wp-content/uploads/2016/06/Mist-scheme-1050x576.png)

Discover more [Hydrosphere Mist use cases](/docs/use-cases/README.md).

-----------------

**[Getting Started Guide and user documentation](/docs/README.md)**

-----------------

## More Features

- Spark Contexts orchestration - Cluster of Spark Clusters: manages multiple Spark contexts in separate JVMs or Dockers
![Cluster of Spark Clusters](http://dv9c7babquml0.cloudfront.net/docs-images/mist-cluster-of-spark-clusters.gif)
- Realtime low latency serving/scoring for ML Lib models. Moved to [spark-ml-serving](https://github.com/Hydrospheredata/spark-ml-serving) library and [hydro-serving](https://github.com/Hydrospheredata/hydro-serving) project
![Mist Local Serving](http://dv9c7babquml0.cloudfront.net/docs-images/mist-model-serving.jpg)
- Clear end-user REST API
```javascript
    POST v2/api/endpoints/weather-forecast?force=true
    {
        lat: “37.777114”,
        long: “-122.419631”
        radius: 100
    }
```
- Spark **2.1.1** support! 
- Scala and **Python** Spark jobs support
- Support for Spark SQL and Hive
- High Availability and Fault Tolerance
- Self Healing after driver program failure
- Powerful logging

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
- [ ] AWS ECS cloudformation package 
- [ ] AWS EMR cloudformation package
- [ ] Hortonworks Ambari package
- [ ] Kerberos integration
- [ ] DC/OS package
- [ ] Dynamic auto-configurable Spark settings based on jobs history
- [ ] Bi-directional streaming API
- [ ] Spark Structural Streaming API
- [ ] AMQP support


## Contact

Please report bugs/problems to: 
<https://github.com/Hydrospheredata/mist/issues>.

<http://hydrosphere.io/>

[LinkedIn](https://www.linkedin.com/company/hydrospherebigdata)

[Facebook](https://www.facebook.com/hydrosphere.io/)

[Twitter](https://twitter.com/hydrospheredata)

