package mist.api.encoding

trait DefaultEncoders extends PrimitiveEncoders
  with CollectionsEncoder
  with DataFrameEncoding

object DefaultEncoders extends DefaultEncoders
