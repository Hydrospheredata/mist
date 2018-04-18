package mist.api.encoding

import shadedshapeless.Lazy

object generic {

  object extractor extends GenericExtractorInstances {
    def derive[A](implicit ext: Lazy[ObjExt[A]]): ObjExt[A] = ext.value
  }

  object encoder extends GenericEncoderInstances {
    def derive[A](implicit enc: Lazy[JsEncoder[A]]): JsEncoder[A] = enc.value
  }

}
