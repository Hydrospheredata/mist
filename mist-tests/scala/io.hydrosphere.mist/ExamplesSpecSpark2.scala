package io.hydrosphere.mist

import java.nio.file.{Path, Paths, Files}

import org.scalatest.{FunSpec, Matchers}

class ExamplesSpecSpark2 extends FunSpec with MistItTest with Matchers {

  val savePathDir = "./target/it-test/ml-data"

  if (isSpark2) {
    val interface = MistHttpInterface("localhost", 2004)

    it("run simple context") {
      val result = interface.runJob("simple-context",
          "numbers" -> List(1, 2, 3),
          "multiplier" -> 2
      )

      result.success shouldBe true
    }

    it("run binarizer") {
      val modelPath = savePathForJob("binarizer")
      val trainR = interface.runJob(
        "binarizer",
        "savePath" -> modelPath
      )

      assert(trainR.success, trainR)

      val serveR = interface.serve(
        "binarizer",
        "features" -> List(0.1, 0.2, 6.0),
        "modelPath" -> modelPath
      )

      assert(serveR.success, serveR)

      val data = serveR.payload("result").asInstanceOf[List[Map[String, Any]]]
      data should contain allOf (
        Map("feature" -> 0.1, "binarized_feature" -> 0),
        Map("feature" -> 0.2, "binarized_feature" -> 0),
        Map("feature" -> 6, "binarized_feature" -> 1)
      )
    }
  }

  def savePathForJob(s: String): String = savePathDir + "/" + s

  override def beforeAll: Unit = {
    super.beforeAll()

    val path = Paths.get(savePathDir)

    cleanDir(path)
    Files.createDirectories(path)
  }

  private def cleanDir(path: Path): Unit = {
    val file = path.toFile
    if (file.isDirectory) {
      file.listFiles()
        .map(f => Paths.get(f.getAbsolutePath))
        .foreach(cleanDir)
      file.delete()
    } else
      file.delete()
  }
}
