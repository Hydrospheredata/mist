//package mist.api.args
//
//import shadedshapeless._
//import shadedshapeless.labelled.FieldType
//
//trait ArgExtractor[A] { self =>
//
//  def `type`: ArgType
//  def extract(a: Any): ArgExtraction[A]
//}
//
//trait RootExtractor[A] extends ArgExtractor[A] {
//  def `type`: RootArgType
//}
//
//trait Basic {
//
//  def create[A](argType: ArgType)(f: Any => ArgExtraction[A]): ArgExtractor[A] = {
//    new ArgExtractor[A] {
//      override def extract(a: Any): ArgExtraction[A] = f(a)
//      override def `type`: ArgType = argType
//    }
//  }
//
//  def createRoot[A](argType: RootArgType)(f: Any => ArgExtraction[A]): RootExtractor[A] = {
//    new RootExtractor[A] {
//      override def extract(a: Any): ArgExtraction[A] = f(a)
//      override def `type`: RootArgType = argType
//    }
//  }
//
//  def nonNull[A](argType: ArgType)(f: Any => ArgExtraction[A]): ArgExtractor[A] = {
//    create(argType)(any => if (any != null) f(any) else Missing("value is null"))
//  }
//
//}
//
//trait Primitives extends Basic {
//
//  import java.{lang => jl}
//
//  private def typed[A, B](clz: Class[B], atype: ArgType): ArgExtractor[A] = {
//    nonNull(atype) { a =>
//      if (clz.isInstance(a))
//        Extracted(a.asInstanceOf[A])
//      else
//        Missing(s"value $a has wrong type: ${a.getClass}, expected $clz")
//    }
//  }
//
//  implicit val forBool: ArgExtractor[Boolean] = typed(classOf[jl.Boolean], MBoolean)
//  implicit val forInt: ArgExtractor[Int] = typed(classOf[jl.Integer], MInt)
//  implicit val forLong: ArgExtractor[Long] = nonNull(MInt) {
//    case i: Int => Extracted(i.toLong)
//    case l: Long => Extracted(l)
//    case x => Missing(s"value $x can't be converted to double")
//  }
//
//  implicit val forDouble: ArgExtractor[Double] = nonNull(MDouble){
//    case i: Int => Extracted(i.toDouble)
//    case d: Double => Extracted(d)
//    case x => Missing(s"value $x can't be converted to double")
//  }
//
//  implicit val forString: ArgExtractor[String] = typed(classOf[jl.String], MString)
//
//  implicit val forJBool: ArgExtractor[jl.Boolean] = typed(classOf[jl.Boolean], MBoolean)
//  implicit val forJInt: ArgExtractor[jl.Integer] = typed(classOf[jl.Integer], MInt)
//  implicit val forJLong: ArgExtractor[jl.Long] = typed(classOf[jl.Long], MInt)
//  implicit val forJDouble: ArgExtractor[jl.Double] = typed(classOf[jl.Double], MDouble)
//
//}
//
//trait Collections extends Basic {
//
//  implicit def forOption[A](implicit ex: ArgExtractor[A]): ArgExtractor[Option[A]] = {
//    create(MOption(ex.`type`))(any => {
//      if (any == null) Extracted(None)
//      else ex.extract(any) match {
//        case Extracted(v) => Extracted(Option(v))
//        case m: Missing[_] => m.asInstanceOf[Missing[Option[A]]]
//      }
//    })
//  }
//
//  implicit def forSeq[A](implicit tP: ArgExtractor[A]): ArgExtractor[Seq[A]] = {
//    nonNull(MList(tP.`type`)) {
//      case seq: Seq[_] =>
//        val elems = seq.map(x => tP.extract(x))
//        if (elems.exists(_.isMissing)) {
//          Missing("elements contain invalid type")
//        } else {
//          Extracted(elems.collect({case Extracted(v) => v}))
//        }
//      case any => Missing(s"value $any has wrong type: ${any.getClass}")
//    }
//  }
//
//  implicit def forList[A](implicit seq: ArgExtractor[Seq[A]]): ArgExtractor[List[A]] = {
//    nonNull(seq.`type`)(any => seq.extract(any).map(_.toList))
//  }
//
//}
//
//trait CaseClasses extends Basic {
//
//  trait ObjExtractor[A] extends RootExtractor[A] {
//    def `type`: MObj
//  }
//
//  implicit def hnilExt[H <: HNil]: ObjExtractor[HNil] = {
//    new ObjExtractor[HNil] {
//      def extract(a: Any): ArgExtraction[HNil] = Extracted(HNil)
//      def `type`: MObj = MObj.empty
//    }
//  }
//
//  implicit def hlistExt[K <: Symbol, H, T <: HList](
//    implicit
//    witness: Witness.Aux[K],
//    hExt: Lazy[ArgExtractor[H]],
//    tExt: ObjExtractor[T]
//  ): ObjExtractor[FieldType[K, H] :: T] = {
//    new ObjExtractor[FieldType[K, H] :: T] {
//
//      override def extract(a: Any): ArgExtraction[FieldType[K, H] :: T] = {
//        val fieldName: String = witness.value.name
//        a match {
//          case m: Map[_, _] =>
//            val map = m.asInstanceOf[Map[String, Any]]
//            val value = map.getOrElse(fieldName, null)
//            val headR = hExt.value.extract(value)
//            val tailR = tExt.extract(map)
//            (headR, tailR) match {
//              case (Extracted(h), Extracted(tail)) =>
//                Extracted((h :: tail).asInstanceOf[FieldType[K, H] :: T])
//              case (Extracted(_), m2: Missing[_]) =>
//                m2.asInstanceOf[Missing[FieldType[K, H] :: T]]
//              case (m1: Missing[_], Extracted(_)) =>
//                Missing(s"field $fieldName:[${m1.description}]")
//              case (m1: Missing[_], m2: Missing[_]) =>
//                Missing(s"field $fieldName:[${m1.description}]; ${m2.description}")
//            }
//
//          case null => Missing(s"value is missing for $fieldName")
//          case x => Missing(s"invalid type: got $x, expected object")
//        }
//      }
//
//      override def `type`: MObj = {
//        val curr = witness.value.name -> hExt.value.`type`
//        val tail = tExt.`type`
//        MObj(tail.fields :+ curr)
//      }
//    }
//  }
//
//  implicit def labelled[A, H <: HList](
//    implicit
//    labGen: LabelledGeneric.Aux[A, H],
//    extL: Lazy[ObjExtractor[H]]
//  ): RootExtractor[A] = {
//    val ext = extL.value
//    createRoot(ext.`type`)(a => ext.extract(a) match {
//      case Extracted(v) => Extracted(labGen.from(v))
//      case m @ Missing(_) => m.asInstanceOf[Missing[A]]
//    })
//  }
//
//}
//
//object ArgExtractor extends Primitives
//    with Collections
//    with CaseClasses {
//
//  def apply[A](implicit ext: ArgExtractor[A]): ArgExtractor[A] = ext
//
//  def rootFor[A](implicit root: RootExtractor[A]): RootExtractor[A] = root
//}

