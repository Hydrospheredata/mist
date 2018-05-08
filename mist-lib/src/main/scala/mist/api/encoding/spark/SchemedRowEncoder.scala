package mist.api.encoding.spark

import java.util.Locale

import mist.api.data._
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

// based on org.apache.spark.sql.catalyst.json.JacksonGenerator
class SchemedRowEncoder(schema: StructType) extends Serializable {

  import SchemedRowEncoder._

  type SG = SpecializedGetters

  def encode(row: InternalRow): JsData = {
    val fieldsConverters = schema.fields.map(f => converter(f.dataType))
    val fields = schema.fields
    val converted =
      for {
        i <- fields.indices
        field = fields(i)
      } yield {
        val data = if (!row.isNullAt(i)) fieldsConverters(i)(row, i) else JsNull
        field.name -> data
      }
    JsMap(converted.toMap)
  }

  private def converter(d: DataType): (SG, Int) => JsData = d match {
    case NullType =>    (g: SG, i: Int) => JsNull
    case BooleanType => (g: SG, i: Int) => JsBoolean(g.getBoolean(i))
    case ByteType =>    (g: SG, i: Int) => JsNumber(g.getByte(i).toInt)
    case ShortType =>   (g: SG, i: Int) => JsNumber(g.getShort(i).toInt)
    case IntegerType => (g: SG, i: Int) => JsNumber(g.getInt(i))
    case LongType =>    (g: SG, i: Int) => JsNumber(g.getLong(i))
    case FloatType =>   (g: SG, i: Int) => JsNumber(g.getFloat(i).toDouble)
    case DoubleType =>  (g: SG, i: Int) => JsNumber(g.getDouble(i))
    case StringType =>  (g: SG, i: Int) => JsString(g.getUTF8String(i).toString)

    case TimestampType => (g: SG, i: Int) => {
      val s = timestampFormat.format(DateTimeUtils.toJavaTimestamp(g.getLong(i)))
      JsString(s)
    }

    case DateType => (g: SG, i: Int) => {
      val s = dateFormat.format(DateTimeUtils.toJavaDate(g.getInt(i)))
      JsString(s)
    }

    case BinaryType => (g: SG, i: Int) => {
      val s = Base64.encodeBase64String(g.getBinary(i))
      JsString(s)
    }

    case dt: DecimalType => (g: SG, i: Int) => {
      val bigDecimal = g.getDecimal(i, dt.precision, dt.scale).toJavaBigDecimal
      new JsNumber(bigDecimal)
    }

    case st: StructType => (g: SG, i: Int) => {
      val row = g.getStruct(i, st.length)
      val encoder = new SchemedRowEncoder(st)
      encoder.encode(row)
    }

    case at: ArrayType => (g: SG, i: Int) => {
      val conv = converter(at.elementType)
      val arr = g.getArray(i)
      val values = (0 until arr.numElements()).map(idx => {
        if (!arr.isNullAt(idx)) conv(arr, idx) else JsNull
      })
      JsList(values)
    }

    case mt: MapType => (g: SG, i: Int) => {
      val map = g.getMap(i)
      val keyArray = map.keyArray()
      val valueArray = map.valueArray()
      val valConv = converter(mt.valueType)

      val fields = (0 until map.numElements()).map(idx => {
        val key = keyArray.get(idx, mt.keyType).toString
        val value = if (!valueArray.isNullAt(idx)) valConv(valueArray, idx) else JsNull
        key -> value
      })
      JsMap(fields.toMap)
    }

    case _ => (g: SG, i: Int) =>
      val v = g.get(i, d)
      sys.error(s"Failed to convert value $v (class of ${v.getClass}}) " +
        s"with the type of $d to JSON.")
  }

}

object SchemedRowEncoder {

  val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd", Locale.US)
  val timestampFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSZZ", Locale.US)

}
