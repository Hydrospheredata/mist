import mist.api.all._
import org.apache.spark.SparkContext

object TextSearchExample extends MistFn {

  override def handle: Handle = {
    withArgs(
      arg[String]("filePath"),
      arg[Seq[String]]("filters")
    ).onSparkContext(
      (path: String, filters: Seq[String], sc: SparkContext) => {
        val upper = filters.map(_.toUpperCase)

        sc.textFile(path)
          .filter(s => upper.exists(filter => s.toUpperCase.contains(filter)))
          .collect()
    })
  }
}
