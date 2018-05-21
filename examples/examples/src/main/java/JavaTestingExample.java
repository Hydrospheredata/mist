import static mist.api.jdsl.JDsl.*;

import mist.api.Handle;
import mist.api.MistFn;
import mist.api.RawHandle;
import mist.api.data.*;
import mist.api.encoding.JsEncoder;
import mist.api.jdsl.*;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JavaTestingExample extends MistFn {

    public Args2<List<Integer>, Integer> in = withArgs(intListArg("nums"), intArg("mult", 2));

    public RawHandle<List<Integer>> raw =
            withArgs(intArg("num")).onSparkContext((num, sc) -> {
                List<Integer> nums = Stream.of(0, 10).collect(Collectors.toList());
                return sc.parallelize(nums).map(x -> x * 2).collect();
    });

    @Override
    public Handle handle() {
        return raw.toHandle();
    }

}

