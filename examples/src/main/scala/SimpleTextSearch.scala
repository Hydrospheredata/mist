import io.hydrosphere.mist.lib.MistJob

object SimpleTextSearch extends MistJob {

  def doStuff(parameters: Map[String, Any]): Map[String, Any] = {
    val path: String = parameters("filePath").asInstanceOf[String]
    val filters: List[String] = parameters("filters").asInstanceOf[List[String]]

    var data = context.textFile(path)

    filters.foreach { currentFilter =>
      data = data.filter(line => line.toUpperCase.contains(currentFilter.toUpperCase))
    }

    Map("result" -> data.collect())
  }
}

