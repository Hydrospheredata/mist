## Spark Job at Mist

######Mist Scala Spark Job 

In order to prepare your job to run on Mist you should extend scala `object` from MistJob and implement abstract method *doStuff* :

```scala
def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = ???
def doStuff(context: SQLContext, parameters: Map[String, Any]): Map[String, Any] = ???
def doStuff(context: HiveContext, parameters: Map[String, Any]): Map[String, Any] = ???
```

for Version Spark >= 2.0.0

```scala
def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = ???
def doStuff(context: SparkSession, parameters: Map[String, Any]): Map[String, Any] = ???
```

Example:

```scala
object SimpleContext extends MistJob {
    override def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = {
        val numbers: List[BigInt] = parameters("digits").asInstanceOf[List[BigInt]]
        val rdd = context.parallelize(numbers)
        Map("result" -> rdd.map(x => x * 2).collect())
    }
}
```

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

Import [mist](https://github.com/Hydrospheredata/mist/tree/master/src/main/python) and implement method *doStuff*. 

The following are Spark Contexts aliases to be used for convenience:

```python
job.sc = SparkContext 
job.sqlc = SQL Context 
job.hc = Hive Context
```

for Version Spark >= 2.0.0

```python
job.sc = SparkContext 
job.ss = SparkSession
```



for example:
```python
import mist
class MyJob:
    def __init__(self, job):
        job.sendResult(self.doStuff(job))
    def doStuff(self, job):
        val = job.parameters.values()
        list = val.head()
        size = list.size()
        pylist = []
        count = 0
        while count < size:
            pylist.append(list.head())
            count = count + 1
            list = list.tail()
        rdd = job.sc.parallelize(pylist)
        result = rdd.map(lambda s: 2 * s).collect()
        return result
        
if __name__ == "__main__":
    job = MyJob(mist.Job())
```
