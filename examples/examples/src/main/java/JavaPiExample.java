import static mist.api.jdsl.Jdsl.*;

import mist.api.Handle;
import mist.api.MistFn;
import mist.api.jdsl.JArg;
import mist.api.jdsl.JEncoders;


import java.util.ArrayList;
import java.util.List;

public class JavaPiExample extends MistFn {

    @Override
    public Handle handle() {
        JArg<Integer> samples = intArg("samples").validated(s -> s > 0, "Samples must be positive");
        return withArgs(samples).onSparkContext((n, sc) -> {
            List<Integer> l = new ArrayList<>(n);
            for (int i = 0; i < n ; i++) {
                l.add(i);
            }

            long count = sc.parallelize(l).filter(i -> {
                double x = Math.random();
                double y = Math.random();
                return x*x + y*y < 1;
            }).count();

            double pi = (4.0 * count) / n;
            return pi;
        }).toHandle(JEncoders.doubleEncoder());
    }
}
