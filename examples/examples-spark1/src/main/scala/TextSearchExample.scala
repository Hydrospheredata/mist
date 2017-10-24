import mist.api._
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.SparkContext

object TextSearchExample extends MistJob[Array[String]]{
  import mist.api.args.WithArgsScala.ArgMagnet

  override def defineJob: JobDef[Array[String]] = {
    withArgs(
      arg[String]("filePath") &
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
