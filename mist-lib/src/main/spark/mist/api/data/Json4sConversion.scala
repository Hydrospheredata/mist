package mist.api.data

import scala.util._

trait Json4sConversion {

  /** For running mist jobs directly from spark-submit **/
  def parse(s: String): Try[JsData] = {
    import org.json4s._

    def translateAst(in: JValue): JsData = in match {
      case JNothing => JsNull //???
      case JNull => JsNull
      case JString(s) => JsString(s)
      case JDouble(d) => JsNumber(d)
      case JDecimal(d) => JsNumber(d)
      case JInt(i) => JsNumber(i)
      case JBool(v) => JsBoolean(v)
      case JObject(fields) => JsMap(fields.map({case (k, v) => k -> translateAst(v)}): _*)
      case JArray(elems) => JsList(elems.map(translateAst))
    }

    Try(org.json4s.jackson.JsonMethods.parse(s, useBigDecimalForDouble = true)).map(json4sJs => translateAst(json4sJs))
  }

  def parseRoot(s: String): Try[JsMap] = parse(s).flatMap {
    case m:JsMap => Success(m)
    case _ => Failure(new IllegalArgumentException(s"Couldn't parse js object from input: $s"))
  }

  def formattedString(js: JsData): String = {
    import org.json4s._

    def translateAst(in: JsData): JValue = in match {
      case JsNull => JNull
      case JsString(s) => JString(s)
      case JsNumber(d) =>
        d.toBigIntExact() match {
          case Some(x) => JInt(x)
          case None => JDecimal(d)
        }
      case JsTrue => JBool(true)
      case JsFalse => JBool(false)
      case JsMap(fields) => JObject(fields.map({case (k, v) => k -> translateAst(v)}).toList)
      case JsList(elems) => JArray(elems.map(translateAst).toList)
    }
    org.json4s.jackson.JsonMethods.pretty(translateAst(js))
  }
}

object Json4sConversion extends Json4sConversion
