package io.hydrosphere.mist.api.ml.reader

import java.util

import org.apache.hadoop.conf.Configuration
import parquet.hadoop.api.ReadSupport.ReadContext
import parquet.hadoop.api.{InitContext, ReadSupport}
import parquet.io.api.RecordMaterializer
import parquet.schema.MessageType

class SimpleReadSupport extends ReadSupport[SimpleRecord] {
  override def prepareForRead(configuration: Configuration, map: util.Map[String, String], messageType: MessageType, readContext: ReadContext): RecordMaterializer[SimpleRecord] = {
    new SimpleRecordMaterializer(messageType)
  }

  override def init(context: InitContext): ReadContext = {
    new ReadContext(context.getFileSchema)
  }
}
