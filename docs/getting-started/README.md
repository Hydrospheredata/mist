# Mist Getting Started

## What is Hydrosphere Mist

Hydrosphere Mist is an open-source service for exposing analytics jobs and machine learning models as web services.

With Hydrosphere Mist you can quickly connect Apache Spark programs and machine learning models trained by Apache Spark with end user applications:
Serve (or score) machine learning models from the REST API with low latency and high throughput
Run parametrized Apache Spark jobs from REST API and receive synchronous or asynchronous results
Subscribe to streaming jobs and receive reactive stream of results
Modify (parametrize) streaming jobs on the fly from the application layer

Our goal is to simplify and facilitate delivery of smart end-to-end solutions on top of machine learning stack.

Hydrosphere Mist is:
 - Resilient: master-slave, auto-recoverable, auto-scalable, self healing
 - Cluster friendly: Kubernetes, DC/OS, ECS, custom
 - Hadoop distribution agnostic: Hortonworks, Cloudera, MapR, custom

## Why do I need Hydrosphere Mist
Hydrosphere helps
 - Data Scientists and Big Data Engineers to deploy analytics jobs and machine learning models as web services
 - Web developers to tap into analytics using familiar REST or messaging API
 - Managers facilitate delivery of end-to-end analytics solutions that include data science, data engineering, middleware and application components nicely decoupled and aligned to each other

## Hydrosphere Mist is not:
 - Hydrosphere Mist does not a fork Apache Spark, does not stick with its particular version, does not force using custom DataFrames or SparkContexts and does not customise machine learning model serialisation format. Mist wraps, executes and manages Apache Spark programs, so they could be accessed by REST API. 
 - Hydrosphere Mist does not rely on PMML or other model serialisation format. By default it uses Apache Spark native models from parquet and scores it with MLLib native classes. Also it is designed to use any machine learning model format and custom serving (scoring) layers like H2O, PMML, MLeap. 
 - Hydrosphere Mist is not a deployment tool, not a continuous delivery tool. Mist jobs can be deployed using any existing CI/CD tool or copied manually into HDFS or local file system.
 - Hydrosphere Mist does not require dependencies like PostgreSQL, Apache Kafka or MQTT. It does not require any storage layer and it is agnostic to messaging systems.
 - Hydrosphere Mist does not require applications to be built on Akka, JVM or other particular languages/frameworks. It simplifies building reactive applications on top of Apache Spark using any languages and frameworks.

## Installing Hydrosphere Mist 
### Quick Start
Hydrosphere Mist is a Scala/Akka application distributed as a Docker container. Apache Spark binaries, all the default configs and sample jobs are already packaged into Mist docker container for a quick start.

```
docker run -p 2003:2003 -v /var/run/docker.sock:/var/run/docker.sock -d hydrosphere/mist:master-2.0.0 mist
```

### (Optional) Checking Web UI

Mist UI is available at `http://localhost:2003/ui`

![Hydrosphere Mist UI](http://dv9c7babquml0.cloudfront.net/docs-images/hydrisphere-mist-ui.png)

You could check running workers and jobs as well as execute/debug API routes right from the web console.

### (Optional) Connecting to your existing Apache Spark cluster
If you would like to install Hydrosphere Mist on top of existing Apache Spark installation please follow high level scheme and detailed steps below. 

![Mist Spark Master config](http://dv9c7babquml0.cloudfront.net/docs-images/mist-spark-master.png)

#### (1/3) Creating or copying Mist config file and saving it in `./configs` directory  

```
mkdir configs
curl -o ./configs/mist.conf https://raw.githubusercontent.com/Hydrospheredata/mist/master/configs/docker.conf
```

#### (2/3) Editing a config and specifying an address of your existing Apache Spark master

```
mist.context-defaults.spark-conf = {
  spark.master = "spark://IP:PORT"
}
```

#### (3/3) Running Mist docker container with mounted config file

```
docker run -p 2003:2003 -v /var/run/docker.sock:/var/run/docker.sock -v $PWD/configs/mist.conf:/usr/share/mist/configs/user.conf -d hydrosphere/mist:master-2.0.0 mist
```

## What's next

Learn from use cases and tutorials here:
- [Use Cases & Tutorials](/docs/use-cases/README.md)
    - [Enterprise Analytics Applications](/docs/use-cases/enterprise-analytics.md)
    - [Reactive Applications](/docs/use-cases/reactive.md)
    - [Realtime Machine Learning Applications](/docs/use-cases/ml-realtime.md)
