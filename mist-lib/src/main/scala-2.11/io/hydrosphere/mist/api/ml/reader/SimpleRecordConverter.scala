package io.hydrosphere.mist.api.ml.reader

import java.nio.charset.{Charset, CharsetDecoder}

import parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter}
import parquet.schema.{GroupType, OriginalType, Type}

import scala.collection.JavaConversions._

class SimpleRecordConverter(schema: GroupType, name: String, parent: SimpleRecordConverter) extends GroupConverter {
  val UTF8: Charset = Charset.forName("UTF-8")
  val UTF8_DECODER: CharsetDecoder = UTF8.newDecoder()

  var converters: Array[Converter] = schema.getFields.map(createConverter).toArray[Converter]

  var record: SimpleRecord = _

  private def createConverter(field: Type): Converter = {
    if (field.isPrimitive) {
      val originalType = field.getOriginalType
      originalType match {
        case OriginalType.UTF8 => return new StringConverter(field.getName)
        case _ => Unit
      }

      return new SimplePrimitiveConverter(field.getName)
    }

    new SimpleRecordConverter(field.asGroupType(), field.getName, this)
  }

  override def getConverter(i: Int): Converter = {
    converters(i)
  }

  override def start(): Unit = {
    record = new SimpleRecord()
  }

  override def end(): Unit = {
    if (parent != null) {
      parent.record.add(name, record)
    }
  }

  private class StringConverter(name: String) extends SimplePrimitiveConverter(name) {
    override def addBinary(value: Binary): Unit = {
      record.add(name, value.toStringUsingUTF8)
    }
  }

  private class SimplePrimitiveConverter(name: String) extends PrimitiveConverter {
    override def addBinary(value: Binary): Unit = {
      val bytes = value.getBytes
      if (bytes == null) {
        record.add(name, null)
        return
      }

      try {
        val buffer = UTF8_DECODER.decode(value.toByteBuffer)
        record.add(name, buffer.toString)
      } catch {
        case _: Throwable => Unit
      }
    }

    override def addBoolean(value: Boolean) {
      record.add(name, value)
    }

    override def addDouble(value: Double) {
      record.add(name, value)
    }

    override def addFloat(value: Float) {
      record.add(name, value)
    }

    override def addInt(value: Int) {
      record.add(name, value)
    }

    override def addLong(value: Long) {
      record.add(name, value)
    }
  }

}