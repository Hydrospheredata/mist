package io.hydrosphere.mist

import java.nio.file.{Files, Path, Paths}

import mist.api.data._
import org.scalatest.{FunSpec, Inside, Matchers}

class ExamplesSpecSpark2 extends FunSpec with MistItTest with Matchers with Inside{

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

      inside(serveR.payload) {
        case JsLikeMap(map) =>
          val res = map.get("result").asInstanceOf[JsLikeList]
          res.list should contain allOf (
            JsLikeMap("feature" -> JsLikeDouble(0.1), "binarized_feature" -> JsLikeInt(0)),
            JsLikeMap("feature" -> JsLikeDouble(0.2), "binarized_feature" -> JsLikeInt(0)),
            JsLikeMap("feature" -> JsLikeDouble(6.0), "binarized_feature" -> JsLikeInt(1))
          )
      }
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
