package io.hydrosphere.mist.jobs

import org.scalatest.{FunSuite, Matchers}

class JobFileTest extends FunSuite with Matchers {

  test("file type matching by type") {
    import JobFile._

    fileType("erer/yoyo.py") shouldBe FileType.Python
    fileType("erer/yoyo.jar") shouldBe FileType.Jar
    fileType("mvn://http://host/:: org % name % 0.0.1") shouldBe FileType.Jar

    intercept[Throwable] {
      fileType("abyrvalg")
    }
  }
}
