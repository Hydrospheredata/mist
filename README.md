[![Build Status](https://ci.hydrosphere.io/buildStatus/icon?job=hydrosphere.io/mist/master)](https://ci.hydrosphere.io/job/hydrosphere.io/job/mist/job/master/)
[![Build Status](https://travis-ci.org/Hydrospheredata/mist.svg?branch=master)](https://travis-ci.org/Hydrospheredata)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.hydrosphere/mist-lib_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.hydrosphere/mist-lib_2.11/)
[![Docker Hub Pulls](https://img.shields.io/docker/pulls/hydrosphere/mist.svg)](https://img.shields.io/docker/pulls/hydrosphere/mist.svg)
# Hydrosphere Mist

[![Join the chat at https://gitter.im/Hydrospheredata/mist](https://badges.gitter.im/Hydrospheredata/mist.svg)](https://gitter.im/Hydrospheredata/mist?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[Hydrosphere](http://hydrosphere.io) Mist is a serverless proxy for Spark cluster.
Mist provides a new functional programming framework and deployment model for Spark applications. 

Please see our [quick start guide](https://hydrosphere.io/mist-docs/quick_start.html) and [documentation](https://hydrosphere.io/mist-docs/)

Features:
* **Spark Function as a Service**. Deploy Spark functions rather than notebooks or scripts.
* Spark Cluster and Session management. Fully managed Spark sessions backed by on-demand EMR, Hortonworks, Cloudera, DC/OS and vanilla Spark clusters.
* **Typesafe** programming framework that clearly defines inputs and outputs of every Spark job.
* **REST** HTTP & Messaging (MQTT, Kafka) API for Scala & Python Spark jobs.
* Multi-cluster mode: Seamless Spark cluster on-demand provisioning, autoscaling and termination(**pending**)
![Cluster of Spark Clusters](http://dv9c7babquml0.cloudfront.net/docs-images/mist-cluster-of-spark-clusters.gif)

It creates a unified API layer for building enterprise solutions and microservices on top of a Spark functions.

![Mist use cases](http://dv9c7babquml0.cloudfront.net/docs-images/mist-use-case.png)

## High Level Architecture

![High Level Architecture](http://dv9c7babquml0.cloudfront.net/docs-images/mist-highlevel-architecture.png)

## Contact

Please report bugs/problems to: 
<https://github.com/Hydrospheredata/mist/issues>.

<http://hydrosphere.io/>

[LinkedIn](https://www.linkedin.com/company/hydrospherebigdata)

[Facebook](https://www.facebook.com/hydrosphere.io/)

[Twitter](https://twitter.com/hydrospheredata)
