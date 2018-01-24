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
- artifact - file (.jar or .py) that contains function
- context - settings for worker where function invokes (spark settings + worker mode) 
- endpoint - named entry point for function invocation (function class name, artifact path, default context)
- job - a result of endpoint invocation
- master - mist master application (it manages functions, jobs, workers, expose public interfaces)
- worker - special spark driver application that actually invokes function (mist automatically spawn them)
- mist-cli - command line tool for interations with mist

## Install

You can install Mist from binaries or run it in docker.
All of distributions have default configuration and our examples for a quick start.
Docker image also has Apache Spark binaries for a quick start.

Releases:

- binaries: <http://repo.hydrosphere.io/hydrosphere/static/> 
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


### Running your own function

Mist provides typeful library for writing functions on scala/java.
Also there is command line tool `mist-cli` that provides an easiest way to
deploy function/context/artifacts. For a quick start let use already [prepared project examples](https://github.com/dos65/hello_mist)

We will use `mist-cli` to upload our function using just one command(or you can use [http interface directly](/mist-docs/http_api.html))
That repository contnains:
- simple function example
- build setup (sbt/mvn)
- configuration files for mist-cli

```sh
# install mist-cli
pip install mist-cli
// or
easy_install mist-cli

# clone examples
git clone https://github.com/dos65/hello_mist.git
```

Scala:
```sh
cd hello_mist/scala

# build function and send its settings to mist
sbt package
mist-cli apply -f conf

# run it
curl -d '{"samples": 10000}' "http://localhost:2004/v2/api/endpoints/hello-mist-scala/jobs?force=true"
```

Java:
```sh
cd hello_mist/java

# build function and send its settings to mist
mvn package
mist-cli apply -f conf

# run it
curl -d '{"samples": 10000}' "http://localhost:2004/v2/api/endpoints/hello-mist-java/jobs?force=true"
```

NOTE: here we use `force=true` to get job result in same http req/resp pair,
it could be useful for quick jobs, but you should not use that parameter for long-running jobs
There are additional [reactive interfaces](/mist-docs/reactive_api.html) for jobs running

#### Scala - more details

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

`src/main/scala/HelloMist.scala`:
```tut:silent
import mist.api._
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.SparkContext

// function object
object HelloMist extends MistFn[Double] {

  override def handle = {
    withArgs(
      // declare an input argument with name `samples` and type `Int` and default value `10000`
      // we could call it by sending:
      //  - {} - empty request, n will be taken from default value
      //  - {"samples": 5 } - n will be 5
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

#### Java - more details

`pom.xml`:
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

`src/main/java/HelloMist.java`:
```java
import mist.api.jdsl.JArg;
import mist.api.jdsl.JHandle;
import mist.api.jdsl.JMistFn;
import mist.api.jdsl.RetValues;

import java.util.ArrayList;
import java.util.List;

// function class
public class HelloMist extends JMistFn<Double> {

  @Override
  public JHandle<Double> handle() {
    // declare an input argument with name `samples` and type `Int` and default value `10000`
    // we could call it by sending:
    //  - {} - empty request, n will be taken from default value
    //  - {"samples": 5 } - n will be 5
    return withArgs(intArg("samples", 10000)).
           // declare that we want to have access to mist extras
           // we could use a internal logger from it for debugging
           withMistExtras().
           onSparkContext((n, extras, sc) -> {

         extras.logger().info("Hello Mist started with samples:" + n);
         List<Integer> l = new ArrayList<>(n);
         for (int i = 0; i < n ; i++) {
             l.add(i);
         }

         long count = sc.parallelize(l).filter(i -> {
             double x = Math.random();
             double y = Math.random();
             return x*x + y*y < 1;
         }).count();

         double pi = (4.0 * count) / n;
         return RetValues.of(pi);
    });
  }
}
```

### Connecting to your existing Apache Spark cluster

Exmaples above was intented to quickly meet a user with mist usage basics.
Function invocation was done using on `local` spark node (default).
The next important thing is to invoke them on a spark cluster.
To do that we should configure a new `context` with cluster settings and change it for our endpoint:
Lets continue to use `mist-cli`:
- using your favorite editor we need to create a file with context settings:
  `hello_mist/scala/conf/10_cluster_context.conf`
  ```
  model = Context
  name = cluster_context
  data {
    spark-conf {
      spark.master = "spark://IP:PORT"
    }
  }
  ```
  `spark-conf` is a section where we could set up or tune [spark settings](https://spark.apache.org/docs/latest/configuration.html) for a context

- in `hello_mist/scala/conf/20_endpoint.conf` set `context = cluster_context`
- send changes to mist:
  ```sh
  mist-cli apply -f conf
  ```

If you want use your Yarn or Mesos cluster, there is not something special configuration from Mist side excluding `spark-master` conf.
Please, follow to offical guides([Yarn](https://spark.apache.org/docs/latest/running-on-yarn.html), [Mesos](https://spark.apache.org/docs/latest/running-on-mesos.html))
Mist uses `spark-submit` under the hood, if you need to provide environment variables for it, just set them up before starting Mist
