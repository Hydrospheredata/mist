import mist.api.data.JsData;
import mist.api.data.JsList;
import mist.api.data.JsMap;
import mist.api.data.JsNumber;
import mist.api.jdsl.Args2;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JavaTestingExampleSpec {

    SparkContext spark = null;

    @Test
    public void testIn() {
        List<JsData> nums = Stream.of(1,2,3,4,5)
                .map(JsNumber::of)
                .collect(Collectors.toList());

        JsMap one = JsMap.empty()
               .addField("numbers", JsList.of(nums))
               .addField("mult", JsNumber.of(10));


        JsMap two = JsMap.empty().addField("numbers", JsList.of(nums));

        JavaTestingExample.
//        val in = TestingExample.in
//        in.extract(FnContext(spark, one)) shouldBe oneExp
//        in.extract(FnContext(spark, two)) shouldBe twoExp
//        in.extract(FnContext(spark, JsMap.empty)) shouldBe a[Failed]
    }

    @BeforeClass
    public void beforeAll() {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("mist-lib-test-java" + this.getClass().getSimpleName())
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.ui.disabled", "true");
        spark = new SparkContext(conf);
    }

    @AfterClass
    public void afterAll() {
        spark.stop();
    }
}
