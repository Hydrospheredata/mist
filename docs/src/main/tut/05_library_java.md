---
layout: docs
title: "Java library API"
permalink: lib_java.html
position: 5
---
### Java library API

Definitions:

*Mist Function* is a functional framework that defines particular Spark calculation. Mist Function is a deployable unit for Mist proxy.
 
*Job* - a Spark job triggered by Mist Function.

Mist Library provides a DSL for Mist Functions that could be deployed and executed in Mist.
`JMistFn` is a base interface for function definition.

`JavaPiExample.java`:
```java
import static mist.api.jdsl.Jdsl.*;

import mist.api.Handle;
import mist.api.MistFn;
import mist.api.jdsl.JEncoders;

import java.util.ArrayList;
import java.util.List;

public class JavaPiExample extends MistFn {

    @Override
    public Handle handle() {
        return withArgs(intArg("samples")).onSparkContext((n, sc) -> {
            List<Integer> l = new ArrayList<>(n);
            for (int i = 0; i < n ; i++) {
                l.add(i);
            }

            long count = sc.parallelize(l).filter(i -> {
                double x = Math.random();
                double y = Math.random();
                return x*x + y*y < 1;
            }).count();

            double pi = (4 * count) / n;
            return pi;
        }).toHandle(JEncoders.doubleEncoder());
    }
}
```

#### Build

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

Speaking generally - `JMistFn` represents an interface that provides
function over one of available spark contexts (SparkContext, SparkSession, StreamingContext).

#### Arguments

Internally library dsl is based on `JArg[A]` - its goal is to describe argument type and how to extract it from a request.
So for example: `intArg("n")` means that request's json object should has key `n` with a value that can be converted to integer.
There are following basic methods do define an argument (they have similar names for different types: intArg/stringArg/doubleArg/booleanArg).
For example for integer:
- `intArg(String name)` -  required argument by name
- `intArg(String name, int defaultValue)` - if argument is missed, function will fallback to default value
- `optIntArg(String name)` - function will receive `Optional<Integer>`
- `intListArg(String name)` - function will receive `List<Integer>`

Method `withArgs` accepts from 1 to 21 argument and returns `ArgsN`.
```java
Args1<Integer> one = withArgs(intArg("n"))

Args2<scala.Tuple2<Integer, String>> two = withArgs(intArg("n"), stringArg("str"))

Args3<scala.Tuple3<Iteger, String, Boolean>> three = withArgs(intArg("n"), stringArg"str"), booleanArg("flag"))
```

#### Contexts

Next to complete Mist Function definition we should inject Spark Context.
Mist provides managed Spark Contexts, so developer does not care about context's lifecycle and settings.
There are following methods of `ArgsN` to define Spark Context:
- `onSparkContext`
- `onSparkSession`
- `onSparkSessionWithHive`
- `onStreamingContext`

It accepts function with `n+1` arguments where `n` is count of combined argument plus `SparkContext` at end.
```java
RawHandle<?> fromOne = withArgs(intArg("n")).onSparkContext((n, sc) -> { ... })

RawHandle<?> fromTwo = withArgs(intArg("n"), stringArg("str")).onSparkContext((n, s, sc) -> { ... })

RawHandle<?> fromThree = withArgs(intArg("n"), stringArg("str"), booleanArg("flag"))
    .onSparkContext((n, s, b, sc) -> { ... })
```

If your job doesn't require any arguments, there are similar methods available from `JMistFn`
```java
import static mist.api.jdsl.Jdsl.*;

import mist.api.Handle;
import mist.api.MistFn;
import mist.api.jdsl.JEncoders;

import java.util.ArrayList;
import java.util.List;

public class NoArgsExample extends MistFn {

    @Override
    public Handle handle() {
        return onSparkContext((sc) -> 42).toHandle(JEncoders.intEncoder());
    }
}
```

#### Validation

For example for calculating pi using dartboard method n should be at least positive number.
For that purpose `JArg<A>` has special methods to validate arguments:
- `validated(f: A => Boolean)`
- `validated(f: A => Boolean, explanation: String)`

```java
import static mist.api.jdsl.Jdsl.*;

import mist.api.Handle;
import mist.api.MistFn;
import mist.api.encoding.JsEncoder;
import mist.api.jdsl.JEncoders;

import java.util.ArrayList;
import java.util.List;

public class JavaPiExample extends MistFn {

    @Override
    public Handle handle() {
        JArg<Integer> samples = intArg("samples").validated(s -> s > 0, "Samples must be positive");
        return withArgs(samples).onSparkContext((n, sc) -> {
          ...
        });
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


#### Encoding

Json encoding is based on `mist.api.JsEncoder[A]` and `mist.api.data.JsData` (json ast)
There are implementations for common result types:
```java
import mist.api.jdsl.JEncoders;
```
It supports:
- Void
- primitives: Short, Int, Long, Float, String, Double, Boolean
- collections: List, Map

Also if you need you could define it by your self:
```java
import static mist.api.jdsl.Jdsl.*;

import mist.api.Handle;
import mist.api.MistFn;
import mist.api.data.*;
import mist.api.encoding.JsEncoder;
import mist.api.jdsl.JEncoders;

class Foo {
  public Int a;
  public String b;
  public Foo (int a, String b) {
     this.a = a;
     this.b = b;
  }
}

class FooEncoder implements JsEncoder[Foo] {

   @Override
   public JsData apply(Foo foo) {
     return JsMap.empty()
        .addField("a", JsNumber.of(foo.a))
        .addField("b", JsString.of(foo.b))
   }
}

class MyFn extends MistFn {

  @Override
  public Handle handle() {
    RawHandle<Foo> raw = onSparkContext((sc) -> new Foo(42, "str"));
    raw.toHandle(new FooEncoder());
  }

}
```

#### Mist extras and logging

Every function invocation on Mist has unique id and associated worker. It could be useful in some cases
to have that extra information in a function body.
Also, to be able to log and see what's going on on job side from mist-ui you could just use `org.slf4j.Logger`
or use `mist.api.Logging` mixin:

```java
import static mist.api.jdsl.Jdsl.*;

import mist.api.Handle;
import mist.api.MistFn;
import mist.api.data.*;
import mist.api.encoding.JsEncoder;
import mist.api.jdsl.JEncoders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Hello extends MistFn {

    Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public Handle handle() {
        return withArgs(intArg("samples")).withMistExtras().onSparkContext((n, extras, sc) -> {
           logger.info("Hello from job:" + extras.jobId());
           return (Void) null;
        }).toHandle(JEncoders.empty());
    }
}
```

#### Passing function into spark-submit

`MisFn` trait has a default `main` implementation, so it's possible to pass mist functions into `spark-submit` directly.
- only ensure that `mist-lib` is will be presented in classpath.

#### Testing

If you want to unit-test your function there is an [example](https://github.com/Hydrospheredata/mist/blob/master/examples/examples/src/test/java/JavaTestingExampleTest.java)
