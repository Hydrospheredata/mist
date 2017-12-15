### Scala DSL

Definitions:

*Mist Function* is a functional framework that defines particular Spark calculation. Mist Function is a deployable unit for Mist proxy.
 
*Job* - a Spark job triggered by Mist Function.

Mist Library provides a DSL for Mist Functions that could be deployed and executed in Mist.
`MistFn[A]` is a base interface for function definition.

`PiExample.scala`:
```scala
import mist.api._
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.SparkContext

object PiExample extends MistFn[Double] {

  override def handle: Handle[Double] = {
    withArgs(arg[Int]("samples")).onSparkContext((n: Int, sc: SparkContext) => {
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

#### Build

Add Mist as dependency in your `build.sbt`:

```scala
libraryDependencies += "io.hydrosphere" %% "mist-lib-spark1" % "0.13.0"
// or if you use spark >= 2.0
libraryDependencies += "io.hydrosphere" %% "mist-lib-spark2" % "0.13.0"
```

Maven dependency:

```xml
<dependency>
    <groupId>io.hydrosphere</groupId>
    <artifactId>mist-lib-spark1_2.10</artifactId>
    <version>0.13.0</version>
</dependency>
// or if you use spark >= 2.0
<dependency>
    <groupId>io.hydrosphere</groupId>
    <artifactId>mist-lib-spark2_2.11</artifactId>
    <version>0.13.0</version>
</dependency>
```

#### Overview

Speaking generally - `MistFn[A]` represents an interface that provides
function over one of available spark contexts (SparkContext, SQLContext, ..., SparkSession).
Here `A` is a function result type, that mist can *automatically* convert to json(see Encoders).

#### Arguments

Internally library DSL is based on `ArgDef[A]` - its goal is to describe argument type and how to extract it from a request.
For example: `arg[Int]("n")` means that request's json object should has key `n` with a value that can be converted to integer.
There are following basic methods do define an argument:
- `arg[T](name: String): ArgDef[A]` - required argument by name
- `arg[A](name: String, default: A): ArgDef[A]` - if argument is missed, request function will fallback to default value
- `allArgs: ArgDef[Map[String, Any]]` - takes all arguments presented in request as `Map[String, Any]`

By default library supports following argument types:
- `Boolean`
- `Int`
- `Double`
- `String`
- `Seq[A]` (where A should be one from supported types)
- `Option[A]` (where A should be one from supported types)

Method `withArgs` accepts from 1 to 21 argument and returns `ArgDef`.
```scala
val one = withArgs(arg[Int]("n"))

val two = withArgs(arg[Int]("n"), arg[String]("str"))

val three = withArgs(arg[Int]("n"), arg[String]("str"), arg[Boolean]("flag"))
```

#### Contexts

Next to complete Mist Function definition we should inject Spark Context.
Mist provides managed Spark Contexts, so developer does not care about context's lifecycle and settings.
There are following methods of `ArgDef` to define Spark Context:
- `onSparkContext`
- `onStreamingContext`
- `onSqlContext`
- `onHiveContext`
- `onSparkSession` (for spark >= 2.0.0)

It accepts function with `n+1` arguments where `n` is count of combined argument plus `SparkContext` at end.
```scala
val fromOne = withArgs(arg[Int]("n")).onSparkContext((n: Int, sc: SparkContext) => { ... })

val fromTwo = withArgs(arg[Int]("n"), arg[String]("str")).onSparkContext((n: Int, s: String, sc: SparkContext) => { ... })

val fromThree = withArgs(arg[Int]("n"), arg[String]("str"), arg[Boolean]("flag"))
    .onSparkContext((n: Int, s: String, b: Boolean, sc: SparkContext) => { ... })
```

If your function doesn't require any arguments, there are similar methods available from `MistFn`
```scala

import mist.api._
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.SparkContext

object NoArgsHandler extends MistFn[Int] {

  override def handle: Handle[Int] = {
    onSparkContext((sc: SparkContext) => 42 )
  }
}

```

#### Validation

For example for calculating pi using dartboard method n should be at least positive number.
For that purpose `ArgDef[A]` has special methods to validate arguments:
- `validated(f: A => Boolean)`
- `validated(f: A => Boolean, explanation: String)`

```scala
import mist.api._
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.SparkContext

object PiExample extends MistFn[Double] {

  override def handle: Handle[Double] = {
    withArgs(
      arg[Int]("samples").validated(n => n > 0, "Samples value should be positive")
    ).onSparkContext((n: Int, sc: SparkContext) => {
       ...
    })
  }
}
```

#### Encoding

Mist should be able to return result back to the client (http request, async interfaces) and it requires
that result should be serialized to json. Scala api ensures that it's possible during compilation,
(there should be an instance of `Encoder` that performs serialization).
There are implementations for common result types:
```scala
import mist.api.encoding.DefaultEncoders._
```
It supports:
- Unit
- primitives: Short, Int, Float, String, Double, Boolean
- collections: Array, Seq, Map
- experimental: DataFrame, DataSet - *Warning* - return them only if you sure that they are small, otherwise it will lead to `OutOfMemory`


#### Mist extras

Every function invocation on Mist has unique id and associated worker. It could be useful in some cases
to have that extra information in a function body.
Also mist provides special logger that collect logs on mist-master node, so you can use it to debug your Spark jobs.
These utilities are called `MistExtras`. Example:

```scala
import mist.api._
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.SparkContext

object HelloWorld extends MistFn[Unit] {

  override def handle: Handle[Unit] = {
    withArgs(arg[Int]("samples"))
      .withMistExtras
      .onSparkContext((n: Int, extras: MistExtras, sc: SparkContext) => {
         // import jobId, workerId, logger into scope
         import extras._ 
         logger.info(s"Hello from $jobId")
    })
  }
}
```

### Next
- [Run your Mist Function](/docs/run-job.md)
