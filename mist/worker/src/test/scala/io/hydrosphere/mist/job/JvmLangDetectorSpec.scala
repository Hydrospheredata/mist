package io.hydrosphere.mist.job

import org.scalatest.{FunSpec, Matchers}

class JvmLangDetectorSpec extends FunSpec with Matchers {

  it("should detect lang by class") {
    JvmLangDetector.lang(this.getClass) shouldBe "scala"
    JvmLangDetector.lang(classOf[java.lang.String]) shouldBe "java"

    JvmLangDetector.lang(classOf[io.hydrosphere.mist.job.test.JavaFn]) shouldBe "java"
    JvmLangDetector.lang(test.ScalaFn.getClass) shouldBe "scala"
  }
}
