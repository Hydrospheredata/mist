import static mist.api.jdsl.Jdsl.*;

import mist.api.Handle;
import mist.api.MistFn;
import mist.api.data.*;
import mist.api.encoding.JsEncoder;
import mist.api.jdsl.JEncoders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class JavaSparkContextExample extends MistFn {

    Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public Handle handle() {
        return withArgs(intArg("num")).onSparkContext((num, sc) -> {
            List<Integer> nums = Stream.of(0, 10).collect(Collectors.toList());
            return sc.parallelize(nums).map(x -> x * 2).collect();
        }).toHandle(JEncoders.listEncoderOf(JEncoders.intEncoder()));
    }

}
