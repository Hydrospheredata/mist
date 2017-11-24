### Java DSL

Mist Library provides a way of writing job functions that could be run on Mist.
`JMistJob[A]` is a start point of job definition.

`JavaPiExample.java`:
```java
import mist.api.jdsl.*;

import java.util.ArrayList;
import java.util.List;

public class JavaPiExample extends JMistJob<Double> {

    @Override
    public JJobDef<Double> defineJob() {
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

Speaking generally - `JMistJob[A]` represents an interface that provides
function over one of available spark contexts (SparkContext, SparkSession, StreamingContext).

#### Arguments

Internally library dsl is based on `ArgDef[A]` - its goal is to describe argument type and how to extract it from a request.
So for example: `intArg("n")` means that request's json object should contain key `n` with a value that can be converted to integer.
There are following basic methods do define an argument (they have similar names for different types: intArg/stringArg/doubleArg/booleanArg).
For example for integer:
- `intArg(String name)` -  required argument by name
- `intArg(String name, int defaultValue)` - if argument is missed, function will fallback to default value
- `optIntArg(String name)` - function will receive `Optional<Integer>`
- `intListArg(String name)` - function will receive `List<Integer>`

To define function of several arguments we should be able to combine them.
For that purpose there is method `withArgs`, it accepts from 1 to 21 argument and returns `ArgDef`.
```java
Args1 one = withArgs(intArg("n"))

Args2 two = withArgs(intArg("n"), stringArg("str"))

Args3 three = withArgs(intArg("n"), stringArg"str"), booleanArg("flag"))
```

#### Contexts

Next to complete job definition we should obtain spark and write function's body.
One of the goals of Mist is to manage spark contexts, so you shouldn't care about context's instantiation.
There are following methods of `ArgsN` to do that:
- `onSparkContext`
- `onStreamingContext`

They are just takes function, that takes `n+1` arguments where n is count of combined argument plus spark context at end.
```java
JJobDef<?> fromOne = withArgs(intArg("n")).onSparkContext((n, sc) -> { ... })

JJobDef<?> fromTwo = withArgs(intArg("n"), stringArg("str")).onSparkContext((n, s, sc) -> { ... })

JJobDef<?> fromThree = withArgs(intArg("n"), stringArg("str"), booleanArg("flag"))
    .onSparkContext((n, s, b, sc) -> { ... })
```

If your job doesn't require any arguments, there are similar methods available from `JMistJob`
```java
import mist.api.jdsl.*;

import java.util.ArrayList;
import java.util.List;

public class NoArgsExample extends JMistJob<Integer> {

    @Override
    public JJobDef<Integer> defineJob() {
        return onSparkContext((sc) -> RetValues.of(42));
    }
}
```

#### Encoding

Mist should be able to return result back to call site (http request, async interfaces) and it requires
that result should be serialized to json. For that purposes function should return an instance of `RetValue`.
There are a lot of helper methonds in `mist.api.jdsl.RetValues`


#### Mist extras

Every job invocation on Mist has unique id and performs on some worker. It can be usefull in some situtaions
to known that information at job side.
Also mist provides special logger that collect logs on mist-master node, so you can use it to debug your jobs.
This thing together is called `MistExtras`. Example:

```java
import io.hydrosphere.mist.api.MLogger;
import mist.api.jdsl.*;

public class Hello extends JMistJob<Void> {

    @Override
    public JJobDef<Integer> defineJob() {
        return withArgs(intArg("samples")).withMistExtras().onSparkContext((n, extras, sc) -> {
           MLogger logger = extras.logger();
           logger.info("Hello from job:" + extras.jobId());
           return RetValues.empty();
        })
    }
}
```

