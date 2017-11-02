import io.hydrosphere.mist.api._

object SimpleTextSearch extends MistJob {

  def execute(filePath: String, filters: List[String]): Map[String, Any] = {
    var data = context.textFile(filePath)

    filters.foreach { currentFilter =>
      data = data.filter(line => line.toUpperCase.contains(currentFilter.toUpperCase))
    }

    Map("result" -> data.collect())
  }
}

