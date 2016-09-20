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
- [More docs](/docs/README.md)

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

## Getting Started with Mist

######Dependencies
- jdk = 8
- scala = 2.10.6, (2.11.8 for Spark Version >=2.0.0)
- spark >= 1.5.2 (earlier versions were not tested)
- MQTT Server (optional)

######Running   


		docker run --name mosquitto-$SPARK_VERSION -d ansi/mosquitto

		docker run --link mosquitto-$SPARK_VERSION:mosquitto -p 2003:2003  -d hydrosphere/mist:master-$SPARK_VERSION mist

[more about docker image](https://hub.docker.com/r/hydrosphere/mist/)

or

* Build the project

        git clone https://github.com/hydrospheredata/mist.git
        cd mist
        ./sbt/sbt -DsparkVersion=1.5.2 assembly # change version according to your installed spark
    
* Create [configuration file](#configuration)
* Run

        ./mist.sh   --config /path/to/application.conf \
                    --jar target/scala-2.10/mist-assembly-0.4.0.jar

## Development mode

You can use Vagrant and work in a preconfigured virtual machine.

```sh
git clone https://github.com/Hydrospheredata/mist
vagrant up
vagrant ssh
cd /vagrant
./sbt/sbt -Dconfig.file=/vagrant/configs/vagrant.conf -Dmist.settings.single-jvm-mode=true run
```

Use Vagrantfile to configure port forwarding and other network setup to make Mist available externally.


## Version Information

| Mist Version   | Scala Version  | Python Version | Spark Version    |
|----------------|----------------|----------------|------------------|
| 0.1.4          | 2.10.6         | 2.7.6          | >=1.5.2          |
| 0.2.0          | 2.10.6         | 2.7.6          | >=1.5.2          |
| 0.3.0          | 2.10.6         | 2.7.6          | >=1.5.2          |
| 0.4.0          | 2.10.6, 2.11.8 | 2.7.6          | >=1.5.2          |
| master         | 2.10.6, 2.11.8 | 2.7.6          | >=1.5.2          |


## Roadmap

-----------------
- [x] Persist job state for self healing
- [x] Super parallel mode: run Spark contexts in separate JVMs
- [x] Powerful logging
- [ ] RESTification
- [ ] Support streaming contexts/jobs
- [ ] Reactive API
- [ ] Cluster mode and Mesos node framework
- [ ] Apache Kafka support
- [ ] AMQP support
- [ ] Web Interface

## Contact

Please report bugs/problems to: 
<https://github.com/Hydrospheredata/mist/issues>.

<http://hydrosphere.io/>

[LinkedIn](https://www.linkedin.com/company/hydrospherebigdata)

[Facebook](https://www.facebook.com/hydrosphere.io/)

[Twitter](https://twitter.com/hydrospheredata)

