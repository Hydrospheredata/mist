import io.hydrosphere.mist.api.{ContextSupport, MistJob}

object SimpleTextSearch extends MistJob with ContextSupport {

  def execute(filePath: String, filters: List[String]): Map[String, Any] = {
    var data = context.textFile(filePath)

    filters.foreach { currentFilter =>
      data = data.filter(line => line.toUpperCase.contains(currentFilter.toUpperCase))
    }

    Map("result" -> data.collect())
  }
}

