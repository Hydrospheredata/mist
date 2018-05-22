import mist.api.Extraction;
import mist.api.FnContextBuilder;
import mist.api.data.JsData;
import mist.api.data.JsList;
import mist.api.data.JsMap;
import mist.api.data.JsNumber;
import mist.api.jdsl.Args2;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.*;
import scala.Tuple2;
import scala.util.Try;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class JavaTestingExampleTest {

    static SparkContext spark = null;

    @Test
    public void testIn() {
        List<JsData> nums = Stream.of(1,2,3,4,5)
                .map(JsNumber::of)
                .collect(Collectors.toList());

        JsMap one = JsMap.empty()
               .addField("nums", JsList.of(nums))
               .addField("mult", JsNumber.of(10));

        JsMap two = JsMap.empty().addField("nums", JsList.of(nums));

        Args2<List<Integer>, Integer> in = JavaTestingExample.in;

        Extraction<Tuple2<List<Integer>, Integer>> out1 = in.extract(FnContextBuilder.create(spark, one));
        assertTrue("out1", out1.isExtracted());
        assertEquals("out1", scala.Tuple2.apply(Arrays.asList(1, 2, 3, 4, 5), 10), out1.get());

        Extraction<Tuple2<List<Integer>, Integer>> out2 = in.extract(FnContextBuilder.create(spark, two));
        assertTrue("out2", out2.isExtracted());
        assertEquals("out2", scala.Tuple2.apply(Arrays.asList(1, 2, 3, 4, 5), 2), out2.get());


        Extraction<Tuple2<List<Integer>, Integer>> out3 = in.extract(FnContextBuilder.create(spark, JsMap.empty()));
        assertFalse("out3", out3.isExtracted());
    }

    @Test
    public void testRaw() {
        List<JsData> nums = Stream.of(1,2,3,4,5)
                .map(JsNumber::of)
                .collect(Collectors.toList());

        JsMap params = JsMap.empty()
                .addField("nums", JsList.of(nums))
                .addField("mult", JsNumber.of(10));

        Try<List<Integer>> out = JavaTestingExample.raw().invoke(FnContextBuilder.create(spark, params));
        assertTrue(out.isSuccess());
        List<Integer> result = out.get();

        assertEquals(Arrays.asList(10, 20, 30 ,40 ,50), result);
    }

    @Test
    public void testFully() {
        List<JsData> nums = Stream.of(1,2,3,4,5)
                .map(JsNumber::of)
                .collect(Collectors.toList());

        JsMap params = JsMap.empty()
                .addField("nums", JsList.of(nums))
                .addField("mult", JsNumber.of(10));

        Try<JsData> out = new JavaTestingExample().handle().invoke(FnContextBuilder.create(spark, params));
        assertTrue(out.isSuccess());
        JsData result = out.get();

        List<JsData> outNums = Stream.of(1,2,3,4,5)
                .map(x -> x * 10)
                .map(JsNumber::of)
                .collect(Collectors.toList());

        JsData exp =  JsList.of(outNums);
        assertEquals(exp, result);
    }

    @BeforeClass
    public static void beforeAll() {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("mist-lib-test-java" + JavaTestingExampleTest.class.getSimpleName())
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.ui.disabled", "true");
        spark = new SparkContext(conf);
    }

    @AfterClass
    public static void afterAll() {
        spark.stop();
    }
}
