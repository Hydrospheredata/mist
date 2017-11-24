import io.hydrosphere.mist.api.MLogger;
import mist.api.jdsl.*;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class JavaSparkContextExample extends JMistJob<List<Integer>> {

    @Override
    public JJobDef<List<Integer>> defineJob() {
        JJobDef<List<Integer>> job =
            withArgs(intArg("num")).
            withMistExtras().
            onSparkContext((num, extras, sc) -> {
                MLogger logger = extras.logger();
                logger.info("Hello from job:" + extras.jobId());

                List<Integer> nums = Stream.of(0, 10).collect(Collectors.toList());
                List<Integer> result = sc.parallelize(nums).map(x -> x * 2).collect();
                return RetValues.of(result);
        });
        return job;
    }

}
