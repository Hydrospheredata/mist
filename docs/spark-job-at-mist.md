## Spark Job at Mist

###### Building Mist jobs

Mist library artifacts have difference between spark versions.

|spark version |artifact        |import                                     |
|--------------|----------------|-------------------------------------------|
| < 2.0        | mist-lib-spark1| `import io.hydrosphere.mist.lib.spark1._` | 
| >= 2.0       | mist-lib-spark2| `import io.hydrosphere.mist.lib.spark2._` |

Add Mist as dependency in your `build.sbt`:

```scala
resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies += "io.hydrosphere" %% "mist-lib-spark1" % "0.11.0"
// or if you use spark >= 2.0
libraryDependencies += "io.hydrosphere" %% "mist-lib-spark2" % "0.11.0"
```

Maven dependency:

```xml
<dependency>
    <groupId>io.hydrosphere</groupId>
    <artifactId>mist-lib-spark1_2.10</artifactId>
    <version>0.11.0</version>
</dependency>
// or if you use spark >= 2.0
<dependency>
    <groupId>io.hydrosphere</groupId>
    <artifactId>mist-lib-spark2_2.11</artifactId>
    <version>0.11.0</version>
</dependency>
```
    
Link for direct download if you don't use a dependency manager:
* http://central.maven.org/maven2/io/hydrosphere/mist/

###### Mist Scala Spark Job 

In order to prepare your job to be executed by Hydrosphere Mist you should extend scala `object` from MistJob and implement method `execute(): Map[String, Any]`:

```scala
import io.hydrosphere.mist.lib.spark{1|2}._

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
import io.hydrosphere.mist.lib.spark${VERSION}._

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
import io.hydrosphere.mist.lib.spark${VERSION}._

object MyOldSparkJob extends MistJob with SQLSupport with HiveSupport {
    def execute(): Map[String, Any] = {
        val dataFrame = sqlContext.read.load("file.parquet") // or hiveContext.read.load("file.parquet")
        ...
        return Map[String, Any].empty
    }
}
```

Inside `execute` method you can write any code you want as it is an ordinary Apache Spark application.


###### Mist Python Spark Job 

Import [mist](https://github.com/Hydrospheredata/mist/tree/master/src/main/resources/mist), extend MistJob class and implement method `def execute(self)`: 

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
