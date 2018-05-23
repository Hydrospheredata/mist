package mist.api

import org.scalatest.{FunSpec, Matchers}

class ExtractionSpec extends FunSpec with Matchers {

  val pos = Extracted(1)
  val neg: Extraction[Int] = Failed.InternalError("err")

  it("should map") {
    val posMap = pos.map(_.toString)
    val negMap = neg.map(_.toString)

    posMap shouldBe Extracted("1")
    negMap shouldBe a[Failed]
  }

  it("should flatMap") {
    val posFail: Extraction[String] = pos.flatMap(i => Failed.InternalError("err"))
    val posSucc = pos.flatMap(i => Extracted(i.toString))
    val negF = neg.flatMap(i => Extracted(i.toString))

    posFail.isExtracted shouldBe false
    posSucc.isExtracted shouldBe true
    negF.isFailed shouldBe true
  }
}
