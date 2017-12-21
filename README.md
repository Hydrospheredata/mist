[![Build Status](https://jenkins.hydrosphere.io/buildStatus/icon?job=hydrosphere/mist/master)](https://jenkins.hydrosphere.io/job/hydrosphere/job/mist/job/master/)
[![Build Status](https://travis-ci.org/Hydrospheredata/mist.svg?branch=master)](https://travis-ci.org/Hydrospheredata)
[![GitHub version](https://badge.fury.io/gh/hydrospheredata%2Fmist.svg)](https://badge.fury.io/gh/hydrospheredata%2Fmist)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.hydrosphere/mist-lib_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.hydrosphere/mist-lib_2.11/)
[![Docker Hub Pulls](https://img.shields.io/docker/pulls/hydrosphere/mist.svg)](https://img.shields.io/docker/pulls/hydrosphere/mist.svg)
# Hydrosphere Mist

[![Join the chat at https://gitter.im/Hydrospheredata/mist](https://badges.gitter.im/Hydrospheredata/mist.svg)](https://gitter.im/Hydrospheredata/mist?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[Hydrosphere](http://hydrosphere.io) Mist is a serverless proxy for Spark cluster.

Main features:
* **Spark Function as a Service**. Deploy Spark functions rather than nodetebooks or scripts.
* Decoupling of user API from Spark settings, cluster provisioning, resource isolation, sharing and auto-scaling. 
* **REST** HTTP & Messaging (MQTT, Kafka) API for Scala & Python Spark jobs.
* Compatibility with EMR, Hortonworks, Cloudera, DC/OS and vanilla Spark distributions.
* Spark **MLLib serving** that has been moved to [spark-ml-serving](https://github.com/Hydrospheredata/spark-ml-serving) library and [ML Lambda](https://github.com/Hydrospheredata/hydro-serving) project

It creates a unified API layer for building enterprise solutions and services on top of a Spark functions.

![Mist use cases](http://hydrosphere.io/wp-content/uploads/2016/06/Mist-scheme-1050x576.png)

Discover more [Hydrosphere Mist use cases](/docs/use-cases/README.md).

-----------------

**[Getting Started Guide and user documentation](https://hydrosphere.io/mist-docs/)**

-----------------

## More Features

- Spark Contexts orchestration - Cluster of Spark Clusters: manages multiple Spark contexts in separate JVMs or Dockers
- Seamless Spark cluster on-demand provisioning, autoscaling and termination
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


## Contact

Please report bugs/problems to: 
<https://github.com/Hydrospheredata/mist/issues>.

<http://hydrosphere.io/>

[LinkedIn](https://www.linkedin.com/company/hydrospherebigdata)

[Facebook](https://www.facebook.com/hydrosphere.io/)

[Twitter](https://twitter.com/hydrospheredata)

