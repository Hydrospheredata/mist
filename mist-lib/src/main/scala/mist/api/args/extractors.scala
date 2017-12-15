package mist.api.args

import shadedshapeless._
import shadedshapeless.labelled.FieldType

/**
  * Check value type and describe
  */
trait TypedPrimitive[A] extends Serializable {

  def `type`: ArgType
  def extract(a: Any): ArgExtraction[A]
}

/**
  * shapeless-like Typeable with our inner type signature
  */
object TypedPrimitive {

  import java.{lang => jl}

  private def typed[A, B](clz: Class[B], atype: ArgType): TypedPrimitive[A] = {
    new TypedPrimitive[A] {
      override def `type`: ArgType = atype
      override def extract(a: Any): ArgExtraction[A] = {
        if (a != null && clz.isInstance(a))
          Extracted(a.asInstanceOf[A])
        else
          Missing(s"value $a has wrong type: ${a.getClass}, expected $clz")
      }
    }
  }

  implicit val boolTyped: TypedPrimitive[Boolean] = typed(classOf[jl.Boolean], MBoolean)
  implicit val shortTyped: TypedPrimitive[Short] = typed(classOf[jl.Short], MInt)
  implicit val intTyped: TypedPrimitive[Int] = typed(classOf[jl.Integer], MInt)
  implicit val longTyped: TypedPrimitive[Long] = typed(classOf[jl.Long], MInt)
  implicit val floatTyped: TypedPrimitive[Float] = typed(classOf[jl.Float], MDouble)
  implicit val doubleTyped: TypedPrimitive[Double] = typed(classOf[jl.Double], MDouble)
  implicit val stringTyped: TypedPrimitive[String] = typed(classOf[jl.String], MString)

  implicit val boolTypedJ: TypedPrimitive[jl.Boolean] = typed(classOf[jl.Boolean], MBoolean)
  implicit val shortTypedJ: TypedPrimitive[jl.Short] = typed(classOf[jl.Short], MInt)
  implicit val intTypedJ: TypedPrimitive[jl.Integer] = typed(classOf[jl.Integer], MInt)
  implicit val longTypedJ: TypedPrimitive[jl.Long] = typed(classOf[jl.Long], MInt)
  implicit val floatTypedJ: TypedPrimitive[jl.Float] = typed(classOf[jl.Float], MDouble)
  implicit val doubleTypedJ: TypedPrimitive[jl.Double] = typed(classOf[jl.Double], MDouble)
}


/**
  *
  * Interpret argument state + Typecasts
  */
trait PlainExtractor[A] extends Serializable {

  def `type`: ArgType
  def extract(a: Option[Any]): ArgExtraction[A]

}

trait LowPrioPlainExtractor {

  protected def create[A](atype: ArgType)(f: Option[Any] => ArgExtraction[A]): PlainExtractor[A] = {
    new PlainExtractor[A] {
      override def `type`: ArgType = atype
      override def extract(a: Option[Any]): ArgExtraction[A] = f(a)
    }
  }

  implicit def primitive[A](implicit tP: TypedPrimitive[A]): PlainExtractor[A] =
    create(tP.`type`) {
      case Some(v) => tP.extract(v)
      case None => Missing("value is missing")
    }
}

trait OptSeqPlainExtractor extends LowPrioPlainExtractor {

  implicit def optionExt[A](implicit tP: TypedPrimitive[A]): PlainExtractor[Option[A]] = {
    create(MOption(tP.`type`)) {
      case Some(v) =>
        tP.extract(v) match {
          case m: Missing[_] => m.asInstanceOf[Missing[Option[A]]]
          case Extracted(e) => Extracted(Option(e))
        }
      case None => Extracted(None)
    }
  }

  implicit def seqExt[A](implicit tP: TypedPrimitive[A]): PlainExtractor[Seq[A]] = {
    create(MList(tP.`type`)) {
      case Some(seq: Seq[_]) =>
        val elems = seq.map(x => tP.extract(x))
        if (elems.exists(_.isMissing)) {
          Missing("elements contain invalid type")
        } else {
          Extracted(elems.collect({case Extracted(v) => v}))
        }
      case Some(any) => Missing(s"value $any has wrong type: ${any.getClass}")
      case None => Missing("missing value")
    }
  }
}

object PlainExtractor extends OptSeqPlainExtractor

/**
  * Support case class extraction
  */
trait LabeledExtractor[A] extends Serializable {
  def info: Seq[UserInputArgument]
  def extract(map: Map[String, Any]): ArgExtraction[A]
}

object LabeledExtractor {

  implicit def hnilExt[H <: HNil]: LabeledExtractor[H] = new LabeledExtractor[H] {
    def info: Seq[UserInputArgument] = Seq.empty
    def extract(map: Map[String, Any]): ArgExtraction[H] = Extracted(HNil.asInstanceOf[H])
  }

  implicit def hlistPlainExt[K <: Symbol, H, T <: HList](
    implicit
    witness: Witness.Aux[K],
    hExt: Lazy[PlainExtractor[H]],
    tExt: LabeledExtractor[T]
  ): LabeledExtractor[FieldType[K, H] :: T] = {
    new LabeledExtractor[FieldType[K, H] :: T] {

      def info: Seq[UserInputArgument] =
        UserInputArgument(witness.value.name, hExt.value.`type`) +: tExt.info

      def extract(map: Map[String, Any]): ArgExtraction[FieldType[K, H] :: T] = {
        val fieldName: String = witness.value.name

        val value = map.get(fieldName)
        val headR = hExt.value.extract(value)

        val tailR = tExt.extract(map)

        (headR, tailR) match {
          case (Extracted(h), Extracted(tail)) =>
            Extracted((h :: tail).asInstanceOf[FieldType[K, H] :: T])
          case (Extracted(_), m2: Missing[_]) =>
            m2.asInstanceOf[Missing[FieldType[K, H] :: T]]
          case (m1: Missing[_], Extracted(_)) =>
            Missing(s"field $fieldName:[${m1.description}]")
          case (m1: Missing[_], m2: Missing[_]) =>
            Missing(s"field $fieldName:[${m1.description}]; ${m2.description}")
        }
      }
    }
  }

  implicit def hlistInnerExt2[K <: Symbol, H, T <: HList](
    implicit
    witness: Witness.Aux[K],
    hExt: Lazy[LabeledExtractor[H]],
    tExt: LabeledExtractor[T]
  ): LabeledExtractor[FieldType[K, H] :: T] = {
    new LabeledExtractor[FieldType[K, H] :: T] {

      def info: Seq[UserInputArgument] = hExt.value.info ++ tExt.info

      def extract(map: Map[String, Any]): ArgExtraction[FieldType[K, H] :: T] = {
        val fieldName: String = witness.value.name

        map.get(fieldName) match {
          case Some(v) if v.isInstanceOf[Map[_, _]] =>
            val casted = v.asInstanceOf[Map[String, Any]]
            val headR = hExt.value.extract(casted)
            val tailR = tExt.extract(map)
            (headR, tailR) match {
              case (Extracted(h), Extracted(tail)) =>
                Extracted((h :: tail).asInstanceOf[FieldType[K, H] :: T])
              case (Extracted(_), m2: Missing[_]) =>
                m2.asInstanceOf[Missing[FieldType[K, H] :: T]]
              case (m1: Missing[_], Extracted(_)) =>
                Missing(s"field $fieldName:[${m1.description}]")
              case (m1: Missing[_], m2: Missing[_]) =>
                Missing(s"field $fieldName:[${m1.description}]; ${m2.description}")
            }

          case x =>
            // build error message and provide information about tail
            val m1 = x match {
              case Some(v) => s"field $fieldName:[value ${v} has wrong type ${v.getClass}, expected Map[String, Any]]]"
              case None => s"field $fieldName:[missing value]"
            }
            tExt.extract(map) match {
              case Extracted(_) => Missing(m1)
              case Missing(m2) => Missing(m1 + ";" + m2)
            }
        }
      }

    }
  }

  implicit def labelled[A, H <: HList](
    implicit
    labGen: LabelledGeneric.Aux[A, H],
    ext: LabeledExtractor[H]
  ): LabeledExtractor[A] = new LabeledExtractor[A] {

    def info: Seq[UserInputArgument] = ext.info

    override def extract(map: Map[String, Any]): ArgExtraction[A] = {
      ext.extract(map) match {
        case Extracted(h) => Extracted(labGen.from(h))
        case m: Missing[_] => m.asInstanceOf[Missing[A]]
      }
    }

  }

}


