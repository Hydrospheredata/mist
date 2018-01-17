---
layout: docs
title: "Quick start"
permalink: quick_start.html
position: 2
---

## Quick start

This tutorial provides a quick introduction to using Mist.
We will first cover local installation details, then briefly overview how to write functions
and finally how to deploy and run them on Mist

To better understainding lets introduce following defenitions that we will use:
- function - user code that invokes on spark
- job - a result of function invocation
- context - settings for worker where function invokes (spark settings + worker mode) 
- artifact - file (.jar or .py) that contains function
- master - mist master application (it manages functions, jobs, workers, expose public interfaces)
- worker - special spark driver application that actually invokes function (mist automatically spawn them)
- mist-cli - command line tool for interations with mist

## Install

You can install Mist from binaries or run it in docker.
All of distributions have default configuration and our examples for a quick start.
Docker image also has Apache Spark binaries for a quick start.

Releases:

- binaries: <http://repo.hydrosphere.io/hydrosphere/static/{{ site.version }}> 
- docker <https://hub.docker.com/r/hydrosphere/mist/>


### Run docker image

We prebuilt Mist for `2.0.0`, `2.1.0`, `2.2.0` Spark versions.
Version of distributive is a combination of Mist and Spark versions.
For example latest Mist release for Spark `2.2.0` version is: `mist:{{ site.version }}-2.2.0`

```sh
docker run -p 2004:2004 \
   -v /var/run/docker.sock:/var/run/docker.sock \
   hydrosphere/mist:{{ site.version }}-2.2.0 mist
```

### Run from binaries

- Download [Spark](https://spark.apache.org/docs/2.1.1/)
- Download Mist and run

```sh
wget http://repo.hydrosphere.io/hydrosphere/static/mist-{{ site.version }}.tar.gz
tar xvfz mist-{{ site.version }}.tar.gz
cd mist-{{ site.version }}

SPARK_HOME=${path to spark distributive} bin/mist-master start --debug true
```

### Check how it works

Mist has build-in UI where you could:
- manage functions
- run jobs, access they results, see addtitional info (status, logs)
- see worker settings and stop them manually

By default ui is available at <http://localhost:2004/ui>.

Demo:
<video autoplay="autoplay">
 <source src="/mist-docs/img/quick-start-ui.webm" type='video/webm; codecs="vp8, vorbis"'>
</video>

Also Mist has prebuilt examples for: 
- [scala/java](https://github.com/Hydrospheredata/mist/tree/master/examples/examples/src/main/)
- [python](https://github.com/Hydrospheredata/mist/tree/master/examples/examples-python)


You can run these examples from web ui, REST HTTP or Messaging API.
For example http call for [SparkContextExample](https://github.com/Hydrospheredata/mist/blob/master/examples/examples/src/main/scala/SparkContextExample.scala)
from examples looks like that:
```sh
curl -d '{"numbers": [1, 2, 3]}' "http://localhost:2004/v2/api/endpoints/spark-ctx-example/jobs?force=true"
```

NOTE: here we use `force=true` to get job result in same http req/resp pair, it can be useful for quick jobs, but you should not use that parameter for long-running jobs

### Running your own function

Mist provides typeful library for writing functions on scala/java

#### Scala

build.sbt:
```scala
lazy val sparkVersion = "2.2.0"
libraryDependencies ++= Seq(
  "io.hydrosphere" %% "mist-lib" % "{{ site.version }}",

  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
)
```

Lets write a function spark which will calculate pi on spark and
quickly overview how to write functions.

`src/main/scala/HelloMist.scala`:
```tut:silent
import mist.api._
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.SparkContext

// an function object
object HelloMist extends MistFn[Double] {

  override def handle = {
    withArgs(
      // declare an input argument with name `n` and type `Int` and default value `10000`
      // we could call it by sending:
      //  - {} - empty request, n will be taken from default value
      //  - {"n": 5 } - n will be 5
      arg[Int]("samples", 10000)
    )
    // declare that we want to have access to mist extras
    // we could use a internal logger from it for debugging
    .withMistExtras
    .onSparkContext((n: Int, extras: MistExtras, sc: SparkContext) => {
      import extras._

      logger.info(s"Hello Mist started with samples: $n")

      val count = sc.parallelize(1 to n).filter(_ => {
        val x = math.random
        val y = math.random
        x * x + y * y < 1
      }).count()

      val pi = (4.0 * count) / n
      pi
    })
  }
}
```

Next we should build an artifact with out function and set it up on mist:
```sh
sbt package
curl 
```


#### Java

For maven:
```xml
  <properties>
    <spark.version>2.2.0</spark.version>
    <java.version>1.8</java.version>
  </properties>

  <dependencies>
    <dependency>
      <groupId>io.hydrosphere</groupId>
      <artifactId>mist-lib_2.11</artifactId>
      <version>1.0.0-RC5</version>
    </dependency>

    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-core_2.11</artifactId>
      <version>${spark.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-sql_2.11</artifactId>
      <version>${spark.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-hive_2.11</artifactId>
      <version>${spark.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-streaming_2.11</artifactId>
      <version>${spark.version}</version>
    </dependency>
  </dependencies>
```




Prerequiments:
- install `mist-cli`
  ```sh
  pip install mist-cli
  // or
  easy_install mist-cli
  ```
- clone repository with prepared projects
  ```sh
  git clone https://github.com/dos65/hello_mist.git
  ```

We will use `mist-cli` to upload our function using just one command(or you can use [http interface directly](/mist-docs/http_api.html))
That repository contnains:
- simple function example
- build setup (sbt/mvn)
- configuration files for mist-cli

#### Scala

```sh
cd hello_mist/scala
sbt package
mist-cli apply -f conf
```

#### Java
 
```sh
cd hello_mist/java
mvn package
mist-cli apply -f conf
```

### Connecting to your existing Apache Spark cluster

If you would like to install Hydrosphere Mist on top of existing Apache Spark installation,
you should edit a config and specifying an address of your existing Apache Spark master.

For local installation it's placed at `${MIST_HOME}/config/default.conf`.
For docker it's in `my_config/docker.conf`

For standalone cluster your config should looks like that:
```
  mist.context-defaults.spark-conf = {
    spark.master = "spark://IP:PORT"
  }
```
If you want use your Yarn or Mesos cluster, there is not something special configuration from Mist side excluding `spark-master` conf.
Please, follow to offical guides([Yarn](https://spark.apache.org/docs/latest/running-on-yarn.html), [Mesos](https://spark.apache.org/docs/latest/running-on-mesos.html))
Mist uses `spark-submit` under the hood, if you need to provide environment variables for it, just set them up before starting Mist
