package mist.api.args

import org.scalatest.{FunSpec, Matchers}
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.reflect.runtime.universe._

class ArgTypeSpec extends FunSpec with Matchers {

  type AliasOption[T] = Option[T]

  val rawToArg = Table(
    ("RawType", "argType"),
    (typeOf[Int], MTInt),
    (typeOf[Double], MTDouble),
    (typeOf[String], MTString),
    (typeOf[List[Int]], MTList(MTInt)),
    (typeOf[Map[Int, String]], MTMap(MTInt, MTString)),
    (typeOf[Option[Int]], MTOption(MTInt)),
    (typeOf[Any], MTAny),
    (typeOf[Any], MTAny),
    (typeOf[AliasOption[Int]], MTOption(MTInt)),
    (typeOf[Map[List[Option[String]], Int]], MTMap(MTList(MTOption(MTString)), MTInt))
  )

  it("it should extract argType from type") {
    forAll(rawToArg) { (t: Type, a: ArgType) =>
       ArgType.fromType(t) shouldBe a
    }
  }

}
