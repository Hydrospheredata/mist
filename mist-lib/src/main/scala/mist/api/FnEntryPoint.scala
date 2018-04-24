package mist.api

import mist.api.data.{JsData, JsMap}
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

import scala.util._

trait FnEntryPoint {

  def execute(ctx: FnContext): Try[JsData]

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(this.getClass)
    val sc = SparkContext.getOrCreate()

    val inputData = args.headOption match {
      case Some(s) => JsData.parseRoot(s) match {
        case Success(js) => js
        case Failure(e) =>
          logger.error("Couldn't parse input data as json map", e)
          sys.exit(1)
      }
      case None => JsMap.empty
    }

    val fnContext = FnContext(sc, inputData)

    execute(fnContext) match {
      case Success(output) =>
        logger.info("Function completed successfully")
        val formatted = JsData.formattedString(output)
        logger.info("Output data:\n" + formatted)
      case Failure(err) =>
        logger.error("Function completed with error", err)
    }
  }
}
