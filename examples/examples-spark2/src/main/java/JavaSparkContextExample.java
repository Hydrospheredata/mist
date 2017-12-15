import mist.api.jdsl.*;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class JavaSparkContextExample extends JMistFn<List<Integer>> {

    @Override
    public JHandle<List<Integer>> handle() {
        return withArgs(intArg("num")).onSparkContext((num, sc) -> {
            List<Integer> nums = Stream.of(0, 10).collect(Collectors.toList());
            List<Integer> result = sc.parallelize(nums).map(x -> x * 2).collect();
            return RetValues.of(result);
        });
    }

}
