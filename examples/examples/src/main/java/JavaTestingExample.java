import mist.api.Handle;
import mist.api.MistFn;
import mist.api.RawHandle;
import mist.api.jdsl.Args2;
import mist.api.jdsl.JEncoders;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

import static mist.api.jdsl.Jdsl.*;

public class JavaTestingExample extends MistFn {

    public static Args2<List<Integer>, Integer> in = withArgs(intListArg("nums"), intArg("mult", 2));

    public static List<Integer> body(List<Integer> nums, int mult, JavaSparkContext sc) {
        return sc.parallelize(nums).map(x -> x * mult).collect();
    }

    public static RawHandle<List<Integer>> raw() {
        return in.onSparkContext(JavaTestingExample::body);
    }

    @Override
    public Handle handle() {
        return raw().toHandle(JEncoders.listEncoderOf(JEncoders.intEncoder()));
    }

}

