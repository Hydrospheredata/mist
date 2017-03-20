## Spark Job at Mist

######Mist Scala Spark Job 

In order to prepare your job to be executed by Hydrosphere Mist you should extend scala `object` from MistJob and implement method `execute(): Map[String, Any]`:

```scala
import io.hydrosphere.mist.MistJob

object MyCoolMistJob extends MistJob {
    def execute(): Map[String, Any] = {
        val rdd = context.parallelize()
        ...
        return result.asInstance[Map[String, Any]]
    }
} 
```

All subclasses have `context` field which is `SparkContext` instance. Method `execute` must always return a result which will be serialised into API response by Mist.

Spark >= 2.0.0 provides `SparkSession` API. Mist manages Apache Spark sessions as well as contexts. You should use `SQLSupport` and `HiveSupport` Mist traits to add `SparkSession` and `HiveQL` API into your job.

```scala
import io.hydrosphere.mist.{MistJob, SQLSupport, HiveSupport}

object MyCoolSessionJob extends MistJob with SQLSupport with HiveSupport {
    def execute(): Map[String, Any] = {
        val dataFrame = session.read.load("file.parquet")
        ...
        return Map[String, Any].empty
    }
}
```

Spark < 2.0.0 `SQLContext` and `HiveContext` API is accessible through `SQLSupport` and `HiveContext` Mist traits. 

```scala
import io.hydrosphere.mist.{MistJob, SQLSupport, HiveSupport}

object MyOldSparkJob extends MistJob with SQLSupport with HiveSupport {
    def execute(): Map[String, Any] = {
        val dataFrame = sqlContext.read.load("file.parquet") // or hiveContext.read.load("file.parquet")
        ...
        return Map[String, Any].empty
    }
}
```

Inside `execute` method you can write any code you want as it is an ordinary Apache Spark application.

######Building Mist jobs

Add Mist as dependency in your `build.sbt`:

```scala
libraryDependencies += "io.hydrosphere" % "mist" % "0.10.0"
```

Maven dependency:

```xml
<dependency>
    <groupId>io.hydrosphere</groupId>
    <artifactId>mist</artifactId>
    <version>0.10.0</version>
</dependency>
```
    
Link for direct download if you don't use a dependency manager:
* http://central.maven.org/maven2/io/hydrosphere/mist/

######Mist Python Spark Job 

Import [mist](https://github.com/Hydrospheredata/mist/tree/master/src/main/reousrces/mist), extend MistJob class and implement method `def execute(self)`: 

```python
from mist.mist_job import *

class MyPythonMistJob(MistJob):
    
    def execute(self):
        rdd = self.context.parallelize()
        ...
        return dict()
```

Add `WithSQLSupport` and `WithHiveSupport` to get `SparkSession` instance in your subclass.

```python
from mist.mist_job import *

class MySessionJob(MistJob, WithSQLSupport, WithHiveSupport):
    
    def execute(self):
        data_frame = self.session.read("file.parquet")
        ...
        return dict()
```

Or for spark < 2.0.0:

```python
from mist.mist_job import *

class MyOldSparkJob(MistJob, WithSQLSupport, WithHiveSupport):
    
    def execute(self):
        data_frame = self.sql_context.read("file.parquet") # or self.hive_context.read("file.parquet")
        ...
        return dict()
```
