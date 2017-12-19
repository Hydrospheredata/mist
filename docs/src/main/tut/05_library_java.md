---
layout: docs
title: "Java library API"
position: 5
---
### Java library API

Definitions:

*Mist Function* is a functional framework that defines particular Spark calculation. Mist Function is a deployable unit for Mist proxy.
 
*Job* - a Spark job triggered by Mist Function.

Mist Library provides a DSL for Mist Functions that could be deployed and executed in Mist.
`JMistFn[A]` is a base interface for function definition.

`JavaPiExample.java`:
```java
import mist.api.jdsl.*;

import java.util.ArrayList;
import java.util.List;

public class JavaPiExample extends JMistFn<Double> {

    @Override
    public JHandle<Double> handle() {
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
            return RetValues.of(pi);
        });
    }
}
```

#### Build

Add Mist as dependency in your `build.sbt`:

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

Speaking generally - `JMistFn[A]` represents an interface that provides
function over one of available spark contexts (SparkContext, SparkSession, StreamingContext).

#### Arguments

Internally library dsl is based on `Jarg[A]` - its goal is to describe argument type and how to extract it from a request.
So for example: `intArg("n")` means that request's json object should has key `n` with a value that can be converted to integer.
There are following basic methods do define an argument (they have similar names for different types: intArg/stringArg/doubleArg/booleanArg).
For example for integer:
- `intArg(String name)` -  required argument by name
- `intArg(String name, int defaultValue)` - if argument is missed, function will fallback to default value
- `optIntArg(String name)` - function will receive `Optional<Integer>`
- `intListArg(String name)` - function will receive `List<Integer>`

Method `withArgs` accepts from 1 to 21 argument and returns `ArgsN`.
```java
Args1 one = withArgs(intArg("n"))

Args2 two = withArgs(intArg("n"), stringArg("str"))

Args3 three = withArgs(intArg("n"), stringArg"str"), booleanArg("flag"))
```

#### Contexts

Next to complete Mist Function definition we should inject Spark Context.
Mist provides managed Spark Contexts, so developer does not care about context's lifecycle and settings.
There are following methods of `ArgsN` to define Spark Context:
- `onSparkContext`
- `onStreamingContext`

It accepts function with `n+1` arguments where `n` is count of combined argument plus `SparkContext` at end.
```java
JHandle<?> fromOne = withArgs(intArg("n")).onSparkContext((n, sc) -> { ... })

JHandle<?> fromTwo = withArgs(intArg("n"), stringArg("str")).onSparkContext((n, s, sc) -> { ... })

JHandle<?> fromThree = withArgs(intArg("n"), stringArg("str"), booleanArg("flag"))
    .onSparkContext((n, s, b, sc) -> { ... })
```

If your job doesn't require any arguments, there are similar methods available from `JMistFn`
```java
import mist.api.jdsl.*;

import java.util.ArrayList;
import java.util.List;

public class NoArgsExample extends JMistFn<Integer> {

    @Override
    public JHandle<Integer> handle() {
        return onSparkContext((sc) -> RetValues.of(42));
    }
}
```

#### Validation

For example for calculating pi using dartboard method n should be at least positive number.
For that purpose `JArg<A>` has special methods to validate arguments:
- `validated(f: A => Boolean)`
- `validated(f: A => Boolean, explanation: String)`

```java
import mist.api.jdsl.*;

import java.util.ArrayList;
import java.util.List;

public class JavaPiExample extends JMistFn<Double> {

    @Override
    public JHandle<Double> handle() {
        JArg<Integer> samples = intArg("samples").validated(s -> s > 0, "Samples must be positive");
        return withArgs(samples).onSparkContext((n, sc) -> {
          ...
        });
    }
}
```

#### Encoding

Mist should be able to return result back to the client (http request, async interfaces) and it requires
that result should be serialized to json.For that purposes function should return an instance of `RetValue`.
There are a lot of helper methonds in `mist.api.jdsl.RetValues`


#### Mist extras

Every function invocation on Mist has unique id and associated worker. It could be useful in some cases
to have that extra information in a function body.
Also mist provides special logger that collect logs on mist-master node, so you can use it to debug your Spark jobs.
These utilities are called `MistExtras`. Example:

```java
import io.hydrosphere.mist.api.MLogger;
import mist.api.jdsl.*;

public class Hello extends JMistFn<Void> {

    @Override
    public JHandle<Integer> handle() {
        return withArgs(intArg("samples")).withMistExtras().onSparkContext((n, extras, sc) -> {
           MLogger logger = extras.logger();
           logger.info("Hello from job:" + extras.jobId());
           return RetValues.empty();
        })
    }
}
```
