package mist.api.encoding

trait DefaultEncoders extends PrimitiveEncoders with CollectionsEncoder

object DefaultEncoders extends DefaultEncoders
