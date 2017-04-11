package io.hydrosphere.mist.jobs.jar

import org.scalatest.{FunSpec, Matchers}

import scala.reflect.runtime.universe._

class JobArgTypeSpec extends FunSpec with Matchers {

  it("parse from Int") {
    JobArgType.fromType(typeOf[Int]) shouldBe MInt
  }

  it("parse from Double") {
    JobArgType.fromType(typeOf[Double]) shouldBe MDouble
  }

  it("parse from String") {
    JobArgType.fromType(typeOf[String]) shouldBe MString
  }

  it("parse from List") {
    JobArgType.fromType(typeOf[List[Int]]) shouldBe MList(MInt)
  }

  it("parse from Map") {
    JobArgType.fromType(typeOf[Map[Int, String]]) shouldBe MMap(MInt, MString)
  }

  it("parse from Option") {
    JobArgType.fromType(typeOf[Option[Int]]) shouldBe MOption(MInt)
  }

  type AliasOption[T] = Option[T]
  it("parse from alias") {
    JobArgType.fromType(typeOf[AliasOption[Int]]) shouldBe MOption(MInt)
  }

  it("should parse complex") {
    val r = JobArgType.fromType(typeOf[Map[List[Option[String]], Int]])
    r shouldBe MMap(MList(MOption(MString)), MInt)
  }
}
