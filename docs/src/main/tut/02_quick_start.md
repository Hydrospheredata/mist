---
layout: docs
title: "Quick start"
permalink: quick_start.html
position: 2
---

## Quick start

This is a quick start tutorial for Mist.
We will cover local installation details, then discuss how to develop and deploy Mist Functions and launch them in different Spark slusters.

Vocabulary. The next terms are used in this document:
- Function - user code with Spark program to be deployed on Mist
- Artifact - file (.jar or .py) that contains a Function
- Mist Master - an application that manages functions, jobs, workers, and expose public interfaces
- Mist Worker - Spark driver application that actually invokes function (Mist Master automatically spawns its Workers)
- Context - settings for Mist Worker where Function is being executed (spark settings + worker mode) 
- Job - a result of the Function execution
- mist-cli - command line tool for Mist

## Install

You can install Mist from binaries or run it in docker.
Docker image also has Apache Spark binaries for a quick start.

Releases:

- binaries: <http://repo.hydrosphere.io/hydrosphere/static/> 
- docker <https://hub.docker.com/r/hydrosphere/mist/>


### Run docker image

We have prebuilt Mist for `2.0.0`, `2.1.0`, `2.2.0` Spark versions.
Version of distributive is a combination of Mist and Spark versions.
For example the latest Mist release for Spark `2.2.0` version looks like this: `mist:{{ site.version }}-2.2.0`

```sh
docker run -p 2004:2004 \
   -v /var/run/docker.sock:/var/run/docker.sock \
   hydrosphere/mist:{{ site.version }}-2.2.0 mist
```

### Run from binaries

- Download [Spark](https://spark.apache.org/downloads.html)
- Download Mist and run

```sh
wget http://repo.hydrosphere.io/hydrosphere/static/mist-{{ site.version }}.tar.gz
tar xvfz mist-{{ site.version }}.tar.gz
cd mist-{{ site.version }}

SPARK_HOME=${path to spark distributive} bin/mist-master start --debug true
```

### Check how it works

Mist has build-in UI where you can:
- manage functions
- run jobs, check results, see additional info (status, logs, etc)
- manage workers

By default UI is available at <http://localhost:2004/ui>.

Demo:
<video autoplay="autoplay">
 <source src="/mist-docs/img/quick-start-ui.webm" type='video/webm; codecs="vp8, vorbis"'>
</video>


### Build your own function

Mist provides typeful library for writing functions in scala/java.
For a quick start please check out a [demo project](https://github.com/dos65/hello_mist). Demo setup includes:
- simple function example in [Scala](https://github.com/dos65/hello_mist/blob/master/scala/src/main/scala/HelloMist.scala) 
and [Java](https://github.com/dos65/hello_mist/blob/master/java/src/main/java/HelloMist.java)
- build setup (sbt/mvn)
- configuration files for `mist-cli`

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

# run it. ?force=true flag forces syncronous execution. By default function is executed in async mode 
curl -d '{"samples": 10000}' "http://localhost:2004/v2/api/functions/hello-mist-scala/jobs?force=true"
```

Java:
```sh
cd hello_mist/java

# build function and send its settings to mist
mvn package
mist-cli apply -f conf

# run it
curl -d '{"samples": 10000}' "http://localhost:2004/v2/api/functions/hello-mist-java/jobs?force=true"
```

NOTE: here we use `force=true` to get job result syncronously in the same http req/resp pair,
it could be useful for quick jobs, but you should not use that parameter for long-running jobs
Please check [reactive interfaces](/mist-docs/reactive_api.html) API as well.

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

### Connect to your existing Apache Spark cluster

By default Mist is trying to connect `local[*]` Spark Master. See Spark [configuration docs](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls) for more details.
Configuring a particular function to be executed on a different Spark Cluster is as simple as changing `spark.master` configuration parameter for function's `context`:

- create a new `context` file with the following settings:
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

- set `context = cluster_context` in `hello_mist/scala/conf/20_function.conf`
- send changes to mist:
  ```sh
  mist-cli apply -f conf
  ```

Yarn or Mesos cluster settings are managed the same way.
Please, follow to offical guides([Yarn](https://spark.apache.org/docs/latest/running-on-yarn.html), [Mesos](https://spark.apache.org/docs/latest/running-on-mesos.html))
Mist uses `spark-submit` under the hood, if you need to provide environment variables for it, just set it up before launching Mist Worker.
