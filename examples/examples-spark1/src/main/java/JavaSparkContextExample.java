import mist.api.jdsl.*;
import io.hydrosphere.mist.api.MLogger;

import java.util.List;

class JavaSparkContextExample extends JMistFn<List<Integer>> {

    @Override
    public JHandle<List<Integer>> handle() {
        return
            withArgs(intListArg("nums")).
            withMistExtras().
            onSparkContext((nums, extras, sc) -> {
                MLogger logger = extras.logger();
                logger.info("Hello from job:" + extras.jobId());
                List<Integer> result = sc.parallelize(nums).map(x -> x * 2).collect();
                return RetValues.of(result);
            });
    }

}
