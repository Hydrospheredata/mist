## What is Hydrosphere Mist?

Hydrosphere Mist is an open-source Multi-tenancy and Multi-user Spark server.

With Hydrosphere Mist, you can quickly connect Apache Spark programs and machine learning models trained by Apache Spark with end user applications:
 - Run parametrized Apache Spark jobs from REST API and receive synchronous or asynchronous results
 - Subscribe to streaming jobs and receive reactive stream of results
 - Modify (parametrize) streaming jobs on the fly from the application layer
 - Serve (or score) machine learning models from the REST API with low latency and high throughput
  
Please note that real-time serving capabilities has been moved to [spark-ml-serving](https://github.com/Hydrospheredata/spark-ml-serving) library and [hydro-serving](https://github.com/Hydrospheredata/hydro-serving) project.

Our primary goal is to simplify and facilitate delivery of smart end-to-end solutions on top of machine learning stack.

Hydrosphere Mist is:
 - Resilient: master-slave, auto-recoverable, auto-scalable, self-healing
 - Cluster-friendly: Kubernetes, DC/OS, ECS, custom
 - Hadoop distribution agnostic: Hortonworks, Cloudera, MapR, custom

## Why do I need Hydrosphere Mist
Hydrosphere helps:
 - Data Scientists and Big Data Engineers to deploy analytics jobs and machine learning models as web services
 - Web developers to tap into analytics using familiar REST or messaging API
 - Managers to facilitate delivery of end-to-end analytics solutions that include data science, data engineering, middleware and application components nicely decoupled and aligned with each other

## Hydrosphere Mist is not:
 - Hydrosphere Mist does not a fork Apache Spark and it does not stick with its particular version. It neither forces using custom DataFrames or SparkContexts nor does it customise machine learning model serialisation format. Mist wraps, executes and manages Apache Spark programs, so they could be accessed by REST API. 
 - Hydrosphere Mist does not rely on PMML or other model serialisation format. By default, it uses Apache Spark native models from parquet and scores it with MLLib native classes. Also, it is designed to use any machine learning model format and custom serving (scoring) layers like H2O, PMML, MLeap. 
 - Hydrosphere Mist is neither a deployment tool nor a continuous delivery tool. Mist jobs can be deployed using any existing CI/CD tool or copied manually into HDFS or local file system.
 - Hydrosphere Mist does not require dependencies like PostgreSQL, Apache Kafka or MQTT. It does not need any storage layer and it is agnostic to messaging systems.
 - Hydrosphere Mist does not require applications to be built on Akka, JVM or other particular languages/frameworks. It simplifies building reactive applications on top of Apache Spark using any languages and frameworks.

### Next
- [Install Mist](/docs/install.md)
- [Mistify your Spark job](/docs/mist-job.md)
- [Run your Mist Job](/docs/run-job.md)