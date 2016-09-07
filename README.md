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
- [How to build application on a top of Apache Spark](#how-to-build-application)
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

[more about this](https://hub.docker.com/r/hydrosphere/mist/)

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
./sbt/sbt run
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

## How to build application
In this tutorial we’ll learn how to allow end users to interact with your big data analytics models which are deployed on Apache Spark cluster.

In this paper we are going to give you 4 practical steps for exposing your script, model or algorithms written in Apache Spark as a service. Think about data scientist who built the model and exposed this in a manner of Google Prediction provides, so it could be used by other developers to build end user applications.

For more high level architectural considerations and tools review please refer to this [blog post](http://hydrosphere.io/blog/).
Use cases.
Consider the following types of solutions:
	-Applications for business users and tenants - Revenue forecaster, Rides simulator, Campaign optimisation service, Predictive maintenance optimisation service and others.
	-Applications for consumers - Recommendation engines, Shopping cart bots.
	-Utilities for Data Scientist/Analyst - Custom Notebook, ETL builder, Data cleansing app.

####1. Take your awesome Apache Spark program.

So, you have an Apache Spark program you would like to expose to outside world.

Scala / Python (Switch)

```
import org.apache.spark.SparkContext

object LocalWeatherForecastApp {
  def main(args: Array[String]) {
     }
}
```

####2. Wrap it into a Mist job

```
(LocalWeatherForecastApp.scala)

import io.hydrosphere.mist.MistJob
import org.apache.spark.SparkContext

case class Result(point: Map[String, String], cloud: Float, sun: Float, rain: Float, temperature: Int, datetime: String)
case class ResultList(results: List[Result])


object LocalWeatherForecastApp extends MistJob {

 override def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = {

   parameters.foreach(f => println(f.toString()))
   val points = parameters("points").asInstanceOf[List[Map[String, String]]]
   val legs = parameters("legs").asInstanceOf[Map[String, Map[String, Any]]]

   val duration = legs("duration").asInstanceOf[Map[String, Any]]
   val durationValue = duration("value").asInstanceOf[BigInt]
   val distance = legs("distance").asInstanceOf[Map[String, Any]]
   val distanceValue = distance("value").asInstanceOf[BigInt]

   val myTz = DateTimeZone.getDefault()
   val now = new Date()
   val utcString = new DateTime(now).withZone(DateTimeZone.UTC).toString()

   val pointsIt = points.iterator
   var resList = new ListBuffer[Result]()

   val r = scala.util.Random

   for (idx <- 1 to points.length) {
     val currentPoint = pointsIt.next ()
     val timeInPoint = new DateTime (now).withZone (DateTimeZone.UTC).plusSeconds((durationValue / points.length * idx ).toInt)
     val result = new Result (currentPoint, (r.nextFloat * 100).toFloat, (r.nextFloat * 100).toFloat, (r.nextFloat * 100).toFloat, (r.nextInt(30)).toInt, timeInPoint.toString () )
     resList += result
   }

   val answer = new ResultList(resList.toList)

   val obj: JObject =
     ( "parameters" -> answer.results.map {w =>
       ("point" ->
         ("lat" -> w.point("lat").toFloat) ~
           ("lng" -> w.point("lng").toFloat)
         ) ~
         ("sun" -> w.sun) ~
         ("cloud" -> w.cloud) ~
         ("rain" -> w.rain) ~
         ("temperature" -> w.temperature) ~
         ("datetime" -> w.datetime)
     } )

   Map("result" -> obj)
 }
}
```
####3. Update project dependencies

```
(build.sbt)
name := "LocalWeatherForecastApp"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
 "org.apache.spark" %% "spark-core" % sparkVersion,
 "org.apache.spark" %% "spark-sql" % sparkVersion,
 "org.apache.spark" %% "spark-hive" % sparkVersion,
 "io.hydrosphere" %% "mist" % "0.4.0"
)
```
####4. Build and deploy

		sbt package 
		[info] Packaging .../localweatherforecastapp_2.10-1.0.jar …

		docker run --name mosquitto-$SPARK_VERSION -d ansi/mosquitto

		docker run --link mosquitto-$SPARK_VERSION:mosquitto -p 2003:2003  -d hydrosphere/mist:master-$SPARK_VERSION mist

[more about this](https://hub.docker.com/r/hydrosphere/mist/)

####5. Bootstrap UI & enjoy
Congratulations! You have just exposed your awesome local weather forecast model for entire organisation or even for public access. 
Now web developer could build an application in 20 lines of Node.js code:

[link to demo](https://github.com/Hydrospheredata/mist-weather-demo/)

## Contact

Please report bugs/problems to: 
<https://github.com/Hydrospheredata/mist/issues>.

<http://hydrosphere.io/>

[LinkedIn](https://www.linkedin.com/company/hydrospherebigdata)

[Facebook](https://www.facebook.com/hydrosphere.io/)

[Twitter](https://twitter.com/hydrospheredata)

