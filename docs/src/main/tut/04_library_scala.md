---
layout: docs
title: "Scala library API"
permalink: lib_scala.html
position: 4
---
### Scala library API

Definitions:

*Mist Function* is a functional framework that defines particular Spark calculation. Mist Function is a deployable unit for Mist proxy.
 
*Job* - a Spark job triggered by Mist Function.

Mist Library provides a DSL for Mist Functions that could be deployed and executed in Mist.
`MistFn` is a base interface for function definition.

`PiExample.scala`:
```tut:silent
import mist.api._
import mist.api.dsl._
import mist.api.encoding.defaults._
import org.apache.spark.SparkContext

object PiExample extends MistFn {

  override def handle: Handle = {
    withArgs(arg[Int]("samples")).onSparkContext((n: Int, sc: SparkContext) => {
      val count = sc.parallelize(1 to n).filter(_ => {
        val x = math.random
        val y = math.random
        x * x + y * y < 1
      }).count()

      val pi = (4.0 * count) / n
      pi
    }).asHandle
  }
}
```

#### Build

Add Mist as dependency in your `build.sbt`:
Notes: it's required to have following spark modules at compile time:
`spark-core`, `spark-sql`, `spark-hive`, `spark-streaming`

```scala
libraryDependencies ++= Seq(
  "io.hydrosphere" %% "mist-lib" % "{{ site.version }}",

  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided"
)

```

Maven dependency:

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
      <version>{{ site.version }}</version>
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

#### Overview

Speaking generally - `MistFn` represents an interface that provides
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

Or you could use `&`/`combine` to get the same result
```scala
val one = arg[Int]("n")

val two = arg[Int]("n") combine arg[String]("str")

val three = arg[Int]("n") & arg[String]("str") & arg[Boolean]("flag")
```

Also it's possible to use case classes as an argument:

```tut:silent
import mist.api._
import mist.api.dsl._
import mist.api.encoding
import mist.api.encoding._
import mist.api.encoding.defaults._
import org.apache.spark.SparkContext

case class Foo(a: Int, b: String)

object ComplexRootArgFn extends MistFn {

  implicit val fooExt: RootExtractor[Foo] = encoding.generic.extractor[Foo]

  // expected input is json:
  //  {
  //    "a": 42,
  //    "b": "some_str"
  //  }
  override def handle: Handle = {
    withArgs(arg[Foo]).onSparkContext((foo: Foo, sc: SparkContext) => 42 ).asHandle
  }
}
```

More examples:
```tut:silent
import mist.api._
// for primitives and collections
import mist.api.encoding
import mist.api.encoding._
import mist.api.encoding.defaults._

case class Bar(a: Int, b: String)
case class Foo(x: Int, bars: Seq[Bar])

// to derive an extractor for `Foo` we need to also have it for `Bar`
implicit val barExt: JsExtractor[Bar] = encoding.generic.extractor[Bar]
implicit val fooExt: RootExtractor[Foo] = encoding.generic.extractor[Foo]

// define root arg that expects following input:
// {
//   "x": 42,
//   "bars": [
//     {"a": 1, "b": "str"},
//     {"a": 2, "b": "str2"}
//   ]
// }
val rootArg = arg[Foo]

// define just complex arg
// {
//   "foo": {
//      "x": 42,
//      "bars": [
//       {"a": 1, "b": "str"},
//       {"a": 2, "b": "str2"}
//     ]
//   }
// }
val fieldArg = arg[Foo]("foo")
```

If you want to allow to skip field with default values:
```tut:silent
import mist.api._
import mist.api.encoding
import mist.api.encoding._
import mist.api.encoding.defaults._

case class Bar(a: Int, b: String = "default")
implicit val barExt: JsExtractor[Bar] = encoding.generic.extractorWithDefaults[Bar]

case class Foo(x: Int, bar: Bar = Bar(1, "str"))
// JsEncoder for bar is required
implicit val barEnc: JsEncoder[Bar] = encoding.generic.encoder[Bar]
implicit val fooExt: RootExtractor[Foo] = encoding.generic.extractorWithDefaults[Foo]
```

#### Contexts

Next to complete Mist Function definition we should inject Spark Context.
Mist provides managed Spark Contexts, so developer does not care about context's lifecycle and settings.
There are following methods of `ArgDef` to define Spark Context:
- `onSparkContext`
- `onSparkSession`
- `onStreamingContext`
- `onSqlContext`
- `onHiveContext`

It accepts function with `n+1` arguments where `n` is count of combined argument plus `SparkContext` at end.
```scala
val fromOne = withArgs(arg[Int]("n")).onSparkContext((n: Int, sc: SparkContext) => { ... })

val fromTwo = withArgs(arg[Int]("n"), arg[String]("str")).onSparkContext((n: Int, s: String, sc: SparkContext) => { ... })

val fromThree = withArgs(arg[Int]("n"), arg[String]("str"), arg[Boolean]("flag"))
    .onSparkContext((n: Int, s: String, b: Boolean, sc: SparkContext) => { ... })
```

If your function doesn't require any arguments, there are similar methods available from `MistFn`
```tut:silent
import mist.api._
import mist.api.dsl._
import mist.api.encoding.defaults._
import org.apache.spark.SparkContext

object NoArgsFn extends MistFn {

  override def handle: Handle = {
    onSparkContext((sc: SparkContext) => 42 ).asHandle
  }
}
```


#### RawHandle / Handle

Mist should be able to return result back to the client (http request, async interfaces) and it requires
that result should be serialized to json.
After we define argument and function body we receive a `RawHandle[A]` and then we should call
`asHandle` method on it to turn it into `Handle`.

So there is not much difference between them:
- RawHandle[A] represents a function `Json + SparkContext => A`
- Handle represents a function `Json + SparkContext => Json`

```tut:silent
import mist.api._
import mist.api.dsl._
import mist.api.encoding.defaults._
import org.apache.spark.SparkContext

object MyFn extends MistFn {

  override def handle: Handle = {
    val rawHandle: RawHandle[Int] = onSparkContext((sc: SparkContext) => 42 )
    rawHandle.asHandle
  }
}

```

#### Encoding

Json encoding is based on `mist.api.JsEncoder[A]` and `mist.api.data.JsData` (json ast)
There are implementations for common result types:
```scala
import mist.api.encoding.defaults._
```
It supports:
- Unit
- primitives: Short, Int, Long, Float, String, Double, Boolean
- collections: Array, Seq, Map
- experimental: DataFrame, DataSet - *Warning* - return them only if you sure that they are small, otherwise it will lead to `OutOfMemory`
  ```scala
  import mist.api.encoding.spark._
  ```

Also if you need you could define it by your self:
```tut:silent
import mist.api._
import mist.api.data._
import mist.api.dsl._

class Foo(val a: Int, val b: String)

object MyFn extends MistFn {

  implicit val fooEnc: JsEncoder[Foo] = JsEncoder(foo => JsMap("a" -> JsNumber(foo.a), "b" -> JsString("b")))

  override def handle: Handle = {
    val rawHandle: RawHandle[Foo] = onSparkContext((sc: SparkContext) => new Foo(42, "str"))
    rawHandle.asHandle
  }

}
```

For more convenient work with `JsData` there is `JsSyntax`:
```tut:silent
import mist.api.data._
import mist.api.encoding.JsSyntax._
import mist.api.encoding.JsEncoder

class Foo(val a: Int, val b: String)
val fooEnc: JsEncoder[Foo] = JsEncoder(foo => JsMap("a" -> foo.a.js, "b" -> foo.b.js))
```

Also it's possible to automatically derive an encoder for case classes:
```tut:silent
import mist.api._
import mist.api.encoding
import mist.api.encoding._
import mist.api.encoding.defaults._

case class Bar(a: Int, b: String)
case class Foo(x: Int, foos: Seq[Bar])

implicit val barEnc: JsEncoder[Bar] = encoding.generic.encoder[Bar]
implicit val fooEnc: JsEncoder[Foo] = encoding.generic.encoder[Foo]
```

#### Validation

For example for calculating pi using dartboard method n should be at least positive number.
For that purpose `ArgDef[A]` has special methods to validate arguments:
- `validated(f: A => Boolean)`
- `validated(f: A => Boolean, explanation: String)`

```tut:silent
import mist.api._
import mist.api.dsl._
import mist.api.encoding.defaults._
import org.apache.spark.SparkContext

object PiExample extends MistFn {

  override def handle: Handle = {
    withArgs(
      arg[Int]("samples").validated(n => n > 0, "Samples value should be positive")
    ).onSparkContext((n: Int, sc: SparkContext) => {
      val count = sc.parallelize(1 to n).filter(_ => {
        val x = math.random
        val y = math.random
        x * x + y * y < 1
      }).count()

      val pi = (4.0 * count) / n
      pi
    }).asHandle
  }
}
```

#### Mist extras and logging

Every function invocation on Mist has unique id and associated worker. It could be useful in some cases
to have that extra information in a function body.
Also, to be able to log and see what's going on on job side from mist-ui you could just use `org.slf4j.Logger`
or use `mist.api.Logging` mixin:

```tut:silent
import mist.api._
import mist.api.dsl._
import mist.api.encoding.defaults._
import org.apache.spark.SparkContext

object HelloWorld extends MistFn with Logging {

  override def handle: Handle = {
    withArgs(arg[Int]("samples"))
      .withMistExtras
      .onSparkContext((n: Int, extras: MistExtras, sc: SparkContext) => {
         import extras._
         logger.info(s"Hello from $jobId")
    }).asHandle
  }
}
```

#### Passing function into spark-submit

`MisFn` trait has a default `main` implementation, so it's possible to pass mist functions into `spark-submit` directly.
- only ensure that `mist-lib` is will be presented in classpath.

#### Testing

If you want to unit-test your function there is an [example](https://github.com/Hydrospheredata/mist/blob/master/examples/examples/src/test/scala/TestExampleSpec.scala)
