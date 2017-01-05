# Reactive Applications
## Overview
Apache Spark blurs the line between batch and streaming data processing. Hydrosphere Mist complements Apache Spark streaming by providing a reactive API for web applications built on top of data analytics stack.

Streaming architectures usually use MQTT or Kafka to publish streaming results to other microservices. Hydrosphere Mist provides ready-to-use recipe and DSL for implementation of streaming API between Apache Spark streaming job and upstream application.

General architecture is clear but when it comes to actual implementation there are following common gaps:
 - No DSL to define an API between Apache Spark program and upstream application. Kafka and MQTT are just transport layers. 
 - Application can not modify running streaming jobs on the fly. Like apply new alerting rule to existing log stream analytics job.
 
How Hydrosphere Mist helps
 - Any Mist job (long running batch or streaming) could publish results asynchronously and continuously using the same API.
 - [Experimental] Hydrosphere Mist will provide a bi-directional API. So, application will be able to parametrize streaming jobs on the fly.

![Reactive Architecture Scheme](http://dv9c7babquml0.cloudfront.net/docs-images/mist-reactive-scheme.png)

### Old reporting architecture vs. Mistified design
In addition to advance streaming use cases Hydrosphere Mist facilitates the right and innovative approach for data warehouse architectures existed for decades. 

A "classical" reporting use case for Apache Spark or Hadoop is to have reporting job scheduled by cron and save results into Hive, HBase, Cassandra or other storage. Then reporting application or BI tool uses SQL on Hadoop or other solutions to fetch reports from that storage.

![Old school reporting architecture](http://dv9c7babquml0.cloudfront.net/docs-images/classical-reporting-architecture.png)

In fact it's not a responsibility of Apache Spark even to think where to store the report. Reporting app/service knows better where to store it since it's responsible for reading and managing it later. Maybe it will decide to store it in Redshift or index it in Elasticsearch - Apache Spark should not care about that.

![Mist enabled reporting architecture](http://dv9c7babquml0.cloudfront.net/docs-images/mist-reactive-reporting-architecture.png)

Hydrosphere Mist and Apache Kafka are used to decouple offline reporting job from other applications. Going further you could add more services subscribed to the reporting topic: notification, audit and others.

![Mist enable reporting architecture extended](http://dv9c7babquml0.cloudfront.net/docs-images/mist-reactive-reporting-architecture-ext.png)

## Tutorial
Let’s take a use case from [Enterprise Analytics Section](/docs/use-cases/enterprise-analytics.md) and extend it to support streaming architecture. Original batch Mist job was designed to receive filter parameters from user application, process log entries and return a result. Now let’s imagine we would like to setup a realtime alert when error captured in logs. It’s a canonical use case of Apache Spark and other streaming engines. Follow the steps below to build an end-to-end solution.

### (1/6) Converting batch into streaming 

Let’s take a SimpleTextSearch Mist job and modify it to use Spark Streaming context and return asynchronous result to upstream. Note that engineer who writes a code is abstracted from transport layer, he does not care about MQTT, Kafka or any other messaging system he will be using.

````
import io.hydrosphere.mist.lib.{MQTTPublisher, MistJob}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

object StreamingTextSearch extends MistJob with MQTTPublisher {
  override def doStuff(parameters: Map[String, Any]): Map[String, Any] = {
    val filter: String = parameters("filter").asInstanceOf[String]
    val ssc = new StreamingContext(context, Seconds(1))

    val inputStream = ssc.queueStream(...)

    val filtredStream = inputStream.filter(x => x.toUpperCase.contains(filter.toUpperCase))

    filtredStream.foreachRDD{ (rdd, time) =>
      publish(Map(
        "time" -> time,
        "length" -> rdd.collect().length,
        "collection" -> rdd.collect().toList.toString
      ))
    }
    ...
    Map.empty[String, Any]
  }
}
````

A full source code could be found at [https://github.com/Hydrospheredata/mist/blob/master/examples/src/main/scala/StreamingTextSearch.scala](https://github.com/Hydrospheredata/mist/blob/master/examples/src/main/scala/StreamingTextSearch.scala)

### (2/6) Checking Router config
Mist provides a Router abstraction which maps incoming HTTP/Messaging requests and CLI commands into underlying Scala & Python programs with actual Mist Jobs. It allows building user friendly endpoints a by exposing only client specific parameters. System parameters like corresponded Java/Python classpath and Spark Context namespace are all set in Router.

Create or edit file `./configs/router.conf` to add a router for our log search application:
````
streaming-log-search = {
    path = '/jobs/log-streaming.jar', // local or HDFS file path
    className = StreamingTextSearch$',
    namespace = 'streaming'
}
````
Please note that Router config could be edited after Mist start, so you could adjust it later in case of any issues.
  
### (3/6) Starting Mist with MQTT
Check that MQTT is switched on in Mist config.

```
mist.mqtt.on = true
mist.mqtt.host = "localhost"
mist.mqtt.port = 1883
mist.mqtt.subscribe-topic = "foo"
mist.mqtt.publish-topic = "foo"
# Inifinity timeout for Streaming context
mist.context.streaming.timeout = Inf
```

Starting Mist is straightforward. For MQTT it is required just to link an MQTT container.

```
docker run --name mosquitto--2.0.0 -d ansi/mosquitto
#create jobs directory and mount it to Mist. So, you'll be able to copy new jobs there
mkdir jobs
docker run -p 2003:2003 --link mosquitto-2.0.0:mosquitto --name mist -v /var/run/docker.sock:/var/run/docker.sock -v $PWD/configs:/usr/share/mist/configs -v $PWD/jobs:/jobs -d hydrosphere/mist:master-2.0.0 mist
```

### (4/6) Deploying a job
Compile and copy the job binary file into local directory mounted to the Mist docker container or HDFS.

```
sbt clean package
cp ./target/scala-2.11/log-streaming.jar ./jobs/
```

It is possible to start any Mist job using REST endpoint. For the testing and demo purposes it makes more sense to start infinity streaming jobs from web console. 

![Mist start job from UI](http://dv9c7babquml0.cloudfront.net/docs-images/mist-ui-run-streaming-job.png)

Also it is very useful to start system streaming jobs from CLI:

```
docker exec -i mist bash -c "/usr/share/mist/bin/mist start job --config /usr/share/mist/configs/docker.conf --route streaming-log-search —-parameters """{\“filter\”:[\”error\”]}""“
```

The resulting configuration & deployment scheme looks as following:

![Mist Configuration Scheme](http://dv9c7babquml0.cloudfront.net/docs-images/mist-config-scheme.png)

Please note that Mist is a service, so it is not required to be restarted every time you update / deploy a new job or edit Router config. So you can iterate multiple times without restarting Mist. 

### (5/6) Testing
Use MQTT client like MQTTLens Chrome extension to connect to MQTT topic specified in Mist config.
If everything goes well you’ll be able to see incoming messages from Hydrosphere Mist.

![MQTT Client Screenshot](http://dv9c7babquml0.cloudfront.net/docs-images/mist-streaming-mqtt-screenshot.png)

### (6/6) Applying a new filter on the fly
Now imagine a real application when user can define error filters and apply those in realtime. Also error filters might be much more complex than simple regular expression, it might be a machine learning model for anomaly detection and noise filtering. And this could be defined, switched on and off from the client application. These use cases seem pretty basic but currently there no straightforward way to implement those. We are working on bi-directional API which will enable such type of interactions between Apache Spark streaming applications and other microservices. 


### What’s next? 
Read the [next section](/docs/use-cases/ml-realtime.md) to learn how to use machine learning models trained in Apache Spark in online applications.