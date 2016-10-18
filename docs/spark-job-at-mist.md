## Spark Job at Mist

######Mist Scala Spark Job 

In order to prepare your job to run on Mist you should extend scala `object` from MistJob and implement method `doStuff(parameters: Map[String, Any]): Map[String, Any]`:

```scala
import io.hydrosphere.mist.MistJob

object MyCoolMistJob extends MistJob {
    def doStuff(parameters: Map[String, Any]): Map[String, Any] = {
        val rdd = context.parallelize()
        ...
        return result.asInstance[Map[String, Any]]
    }
} 
```

All subclasses have `context` field which is `SparkContext` instance. Method `doStuff` must always return some result: this result Mist will send as response.

In Spark >= 2.0.0 jobs can have `SparkSession`. Mist manages sessions in contexts too. You should add `SQLSupport` as super class to your job to get `SparkSession` instance within it. Besides that you can extend your class with `HiveSupport` to use HiveQL inside `SparkSession`.

```scala
import io.hydrosphere.mist.{MistJob, SQLSupport, HiveSupport}

object MyCoolSessionJob extends MistJob with SQLSupport wiyj HiveSupport {
    def doStuff(parameters: Map[String, Any]): Map[String, Any] = {
        val dataFrame = session.read.load("file.parquet")
        ...
        return Map[String, Any].empty
    }
}
```

There is no sessions but there are `SQLContext` and `HiveContext` in Spark < 2.0.0. You get these instances extending `SQLSupport` and `HiveContext` accordingly. 

```scala
import io.hydrosphere.mist.{MistJob, SQLSupport, HiveSupport}

object MyOldSparkJob extends MistJob with SQLSupport wiyj HiveSupport {
    def doStuff(parameters: Map[String, Any]): Map[String, Any] = {
        val dataFrame = sqlContext.read.load("file.parquet") // or hiveContext.read.load("file.parquet")
        ...
        return Map[String, Any].empty
    }
}
```

Inside `doStuff` method you can write any code you want as if it is ordinary Spark application.

######Building Mist jobs

Add Mist as dependency in your `build.sbt`:

```scala
libraryDependencies += "io.hydrosphere" % "mist" % "0.4.0"
```

Maven dependency:

```xml
<dependency>
    <groupId>io.hydrosphere</groupId>
    <artifactId>mist</artifactId>
    <version>0.4.0</version>
</dependency>
```
    
Link for direct download if you don't use a dependency manager:
* http://central.maven.org/maven2/io/hydrosphere/mist/

######Mist Python Spark Job 

Import [mist](https://github.com/Hydrospheredata/mist/tree/master/src/main/reousrces/mist), extend MistJob class and implement method `def do_stuff(self, params)`: 

```python
from mist.mist_job import *

class MyPythonMistJob(MistJob):
    
    def do_stuff(self, parameters):
        rdd = self.context.parallelize()
        ...
        return dict()
```

As it is in scala you can add `WithSQLSupport` and `WithHiveSupport` to get `SparkSession` instance in your subclass.

```python
from mist.mist_job import *

class MySessionJob(MistJob, WithSQLSupport, WithHiveSupport):
    
    def do_stuff(self, parameters):
        data_frame = self.session.read("file.parquet")
        ...
        return dict()
```

Or for spark < 2.0.0:

```python
from mist.mist_job import *

class MyOldSparkJob(MistJob, WithSQLSupport, WithHiveSupport):
    
    def do_stuff(self, parameters):
        data_frame = self.sql_context.read("file.parquet") # or self.hive_context.read("file.parquet")
        ...
        return dict()
```
