import mist.api._
import mist.api.dsl._
import mist.api.encoding.defaults._
import org.apache.spark.SparkContext

object TestingExample extends MistFn {

  val in = arg[Seq[Int]]("numbers") & arg[Int]("mult", 2)

  def body(nums: Seq[Int], mult: Int, sc: SparkContext): Array[Int] = {
    sc.parallelize(nums).map(x => x * mult).collect()
  }

  val raw = in.onSparkContext(body _)

  override def handle: Handle = raw.asHandle

}
