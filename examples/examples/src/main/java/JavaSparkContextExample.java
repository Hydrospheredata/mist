import static mist.api.jdsl.JDsl.*;

import mist.api.Handle;
import mist.api.MistFn;
import mist.api.data.*;
import mist.api.encoding.JsEncoder;
import mist.api.jdsl.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class Point {

    private int x;
    private int y;

    public Point(int x, int y) {
        this.x = x;
        this.y = y;
    }

    public int getY() {
        return y;
    }

    public int getX() {
        return x;
    }
}

class MyEnc implements JsEncoder<Point> {

    @Override
    public JsData apply(Point point) {
        List<Field> fields = new ArrayList<>();
        fields.add(Field.of("x", JsNumber.of(point.getX())));
        fields.add(Field.of("y", JsNumber.of(point.getY())));
        return JsMap.of(fields);
    }

}

class JavaSparkContextExample extends MistFn {

    @Override
    public Handle handle() {
        return withArgs(intArg("num")).onSparkContext((num, sc) -> {
            List<Integer> nums = Stream.of(0, 10).collect(Collectors.toList());
            return sc.parallelize(nums).map(x -> x * 2).collect();
        }).toHandle(JEncoders.listEncoderOf(JEncoders.intEncoder()));
    }

}
