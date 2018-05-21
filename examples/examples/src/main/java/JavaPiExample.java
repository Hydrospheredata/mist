import static mist.api.jdsl.JDsl.*;
import mist.api.jdsl.JArg;
import mist.api.jdsl.JHandle;
import mist.api.jdsl.JMistFn;
import mist.api.jdsl.RetValues;


import java.util.ArrayList;
import java.util.List;

public class JavaPiExample extends JMistFn {

    @Override
    public JHandle handle() {
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
            return RetValues.of(pi);
        });
    }
}
