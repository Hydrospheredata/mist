import io.hydropsphere.mist.api.v2.java.MistJob;
import io.hydrosphere.mist.api.v2.JobP;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.hydropsphere.mist.api.v2.java.JDsl.intArg;
import static io.hydropsphere.mist.api.v2.java.JDsl.withArgs;

public class SimpleJavaContext implements MistJob {

    @Override
    public JobP<?> defineJob() {
        return withArgs(intArg("n")).withContext2((Integer n, JavaSparkContext sc) -> {
            List<Integer> numbers = IntStream.range(0, n).boxed().collect(Collectors.toList());
            JavaRDD<Integer> rdd = sc.parallelize(numbers);
            return rdd.map((x) -> x * 20).collect();
        });
    }
}
