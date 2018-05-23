import static mist.api.jdsl.Jdsl.*;

import mist.api.Handle;
import mist.api.MistFn;
import mist.api.jdsl.*;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

class JavaStreamingContextExample extends MistFn {

    @Override
    public Handle handle() {
        return withMistExtras().onStreamingContext((extras, jsc) -> {

            List<Integer> list = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                list.add(i);
            }
            Queue<JavaRDD<Integer>> rddQueue = new LinkedList<>();
            for (int i = 0; i < 30; i++) {
                rddQueue.add(jsc.sparkContext().parallelize(list));
            }

            jsc.queueStream(rddQueue)
               .mapToPair(x -> new scala.Tuple2<>(x % 10, 1))
               .reduceByKey((a,b) -> a + b)
               .foreachRDD((rdd, time) -> {
                    List<scala.Tuple2<Integer, Integer>> values = rdd.collect();
                    String msg = "time:" + time + ", length:" + values.size() + ", collection:" + values;
               });

            jsc.start();
            jsc.awaitTerminationOrTimeout(10 * 1000);
            jsc.stop();

            return (Void) null;
        }).toHandle(JEncoders.empty());
    }
}
