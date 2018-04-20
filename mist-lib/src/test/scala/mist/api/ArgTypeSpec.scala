package mist.api

import org.scalatest.{FunSpec, Matchers}
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.reflect.runtime.universe._

class ArgTypeSpec extends FunSpec with Matchers {

  type AliasOption[T] = Option[T]

  val rawToArg = Table(
    ("RawType", "argType"),
    (typeOf[Int], MInt),
    (typeOf[Double], MDouble),
    (typeOf[String], MString),
    (typeOf[List[Int]], MList(MInt)),
    (typeOf[Map[Int, String]], MMap(MInt, MString)),
    (typeOf[Option[Int]], MOption(MInt)),
    (typeOf[Any], MAny),
    (typeOf[Any], MAny),
    (typeOf[Boolean], MBoolean),
    (typeOf[AliasOption[Int]], MOption(MInt)),
    (typeOf[Map[List[Option[String]], Int]], MMap(MList(MOption(MString)), MInt))
  )

  it("it should extract argType from type") {
    forAll(rawToArg) { (t: Type, a: ArgType) =>
       ArgType.fromType(t) shouldBe a
    }
  }

}
