# Enterprise Analytics Applications

## Overview

Currently the most common workflow for big data analytics applications is to pre-process large data sets offline using combination of Apache Spark batch and streaming jobs and then save results into key-value or columnar database. Then enterprise applications use pre-calculated data sets for visualisations, search, email campaigns, reporting and other use cases.  

However this "classic" architecture has number of limitations. We list those below and highlight how Hydrosphere Mist addresses the issues in the context of advance analytics applications.

Classic architecture limitations:
 - Not all the data sets could be pre-calculated in advance.
 - No clear API. Enterprise applications has no control over Apache Spark jobs.
 - Shared nothing architecture principle is broken - shared database is used as an API between analytics stack and enterprise applications.

How Hydrosphere Mist helps
 - User can define additional parameters for the job and execute reporting, simulation or prediction jobs on demand using Hydrosphere Mist API.
 - Applications and services instantly trigger parameterized Apache Spark jobs and receive results via domain specific REST or Messaging API.
 - Every application or service should decide where and how to store/index data set for later use. Hydrosphere Mist enforces the right decoupling of these services.
 
By solving these challenges Hydrosphere Mist enables more advanced and interactive enterprise analytics applications to be built on top of Apache Spark.

Please note in this use case we are not talking about low latency job execution. Obviously the main purpose of using Apache Spark is to process large data sets. Large data sets processing could not be low latency. Hydrosphere Mist garanties instant job start by managing SparkContexts and provides API layer on top of that. 
For realtime applications with low latency and high throughput built on top of machine learning models trained in by Apache Spark please refer to the use case [Realtime Machine Learning Applications](/docs/use-cases/ml-realtime.md).

## Tutorial
In this tutorial we will take the basic Apache Spark example, extend it to more realistic use case and deploy it as a service using Hydrosphere Mist.

### (1/6) Taking basic Apache Spark text search example

Let’s start from text search example from original [Apache Spark tutorials](http://spark.apache.org/examples.html).

````
val textFile = sc.textFile("hdfs://...")

// Creates a DataFrame having a single column named "line"
val df = textFile.toDF("line")
val errors = df.filter(col("line").like("%ERROR%"))
// Counts all the errors
errors.count()
// Counts errors mentioning MySQL
errors.filter(col("line").like("%MySQL%")).count()
// Fetches the MySQL errors as an array of strings
errors.filter(col("line").like("%MySQL%")).collect()
````

### (2/6) Mistifying Spark program
As you see our original Apache Spark program has hardcoded ERROR and MySQL filter strings. Obviously in order to provide a useful log analytics application for the end users it makes sense to expose these parameters in user interface. UI application would pass it through Hydrosphere Mist right into Apache Spark program. Also even more realistic use case might have additional noise filtering using pre-build machine learning models.

It takes 3 lines of code to Mistify the program.

````
import io.hydrosphere.mist.lib.{MistJob}

object LogSearchJob extends MistJob {

  override def doStuff(parameters: Map[String, Any]): Map[String, Any] = {
    val path: String = parameters("filePath").asInstanceOf[String]
    val filters: List[String] = parameters("filters").asInstanceOf[List[String]]

    var data = context.textFile(path)

    filters.foreach { currentFilter =>
      data = data.filter(line => line.toUpperCase.contains(currentFilter.toUpperCase))
    }

    Map("result" -> data.collect())
  }
}
````
A full source code could be found at [https://github.com/Hydrospheredata/mist/blob/master/examples/src/main/scala/SimpleTextSearch.scala](https://github.com/Hydrospheredata/mist/blob/master/examples/src/main/scala/SimpleTextSearch.scala)

Mist job accepts user parameters map, in our case we expect path to the log file and filter names. Then we pass those to the regular Spark program we had before.

### (3/6) Configuring Router

Mist provides a Router abstraction that maps incoming HTTP/Messaging requests and CLI commands into underlying Scala & Python programs with actual Mist Jobs. It allows building user friendly endpoints by exposing only client specific parameters. System parameters like corresponded Java/Python classpath and Spark Context namespace are all set in Router.

Create or edit file `./configs/router.conf` to add a router for the log search application:

````
log-search = {
    path = '/jobs/search-job.jar', // local or HDFS file path
    className = LogSearchJob$',
    namespace = 'production-namespace'
}
````

This route describes REST API endpoint /api/log-search, so client application could send a simple REST request and get result back:
````
POST /api/log-search
{
    "filter": “ERROR”
}
````

### (4/5) Starting Mist 
Start Mist and make sure that Router config you have created in step (3/5) and directory with search-job.jar has been mounted to the Docker container.  

````
mkdir jobs
docker run -p 2003:2003 --name mist -v /var/run/docker.sock:/var/run/docker.sock -v $PWD/configs:/usr/share/mist/configs -v $PWD/jobs:/jobs -d hydrosphere/mist:master-2.0.0 mist
````

### (5/6) Deploying a job

Use `sbt package` or your own favourite build pipeline to package Scala programs. Then copy compiled .jar or Python .py file into the path attribute specified in log-search route (see step 3/5).

```
sbt package
cp ./target/scala-2.11/search-job.jar ./jobs
```

That’s it - all that you need to deploy a job is to copy it to the local or HDFS directory Mist has access to. The resulting configuration & deployment scheme looks as following:

![Mist Configuration Scheme](http://dv9c7babquml0.cloudfront.net/docs-images/mist-config-scheme.png)

Please note that Mist is a service, so it is not required to be restarted every time you update / deploy a new job or edit Router config. So you can iterate multiple times without restarting Mist. 

### (6/6) Testing

Here we go. Let's create a test log file and try your API endpoints using cURL:

```
cat >> ./jobs/text_search.log << EOF
error error mysql
exception
error mongodb
hydrosphere mist no errors
EOF

curl --header "Content-Type: application/json" -X POST http://localhost:2003/api/log-search --data '{"filters": ["ERROR", "MySQL"], "filePath": "/jobs/text_search.log"}'
```

Also you could use Mist web console to test/debug routes.

### Summary
This relatively basic tutorial could be easily extended to support various of enterprise analytics applications such as reporting, simulation (pricing, bank stress testing, taxi rides), forecasting (ad campaign, energy savings, others), ad-hoc analytics tools for business users (hosted notebooks - smart web apps for business users), and others. The technical similarity of these application is Hydrosphere Mist / Apache Spark job exposed as an API to the client application.

### What’s next?
Consider one more use case - what if you would like to process logs in Apache Streaming and alert web application in reactive way. Learn how to build such kind of applications with Hydrosphere Mist in the [next section](/docs/use-cases/reactive.md).