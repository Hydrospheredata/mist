package io.hydrosphere.mist.worker.runners

import java.io.File
import java.nio.file.Paths

import io.hydrosphere.mist.worker.SparkArtifact
import io.hydrosphere.mist.worker.runners.python.PythonRunner
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, FunSpecLike, Matchers}

class RunnerSelectorSpec extends FunSpecLike
  with Matchers
  with BeforeAndAfter {
  val basePath = "./target/runner"
  val pyFile = SparkArtifact(Paths.get(basePath, "test.py").toFile, "url")
  val jarFile = SparkArtifact(Paths.get(basePath, "test.jar").toFile, "url")
  val unknown = SparkArtifact(Paths.get(basePath, "test.unknown").toFile, "url")

  before {
    val f = new File(basePath)
    if (f.exists()) FileUtils.deleteDirectory(f)
    FileUtils.forceMkdir(f)
    FileUtils.touch(pyFile.local)
    FileUtils.touch(jarFile.local)
  }

  after {
    FileUtils.deleteQuietly(pyFile.local)
    FileUtils.deleteQuietly(jarFile.local)
  }

  it("should select runner by extension") {
    val selector = new SimpleRunnerSelector
    selector.selectRunner(pyFile) shouldBe a[PythonRunner]
    selector.selectRunner(jarFile) shouldBe a[ScalaRunner]
  }


  it("should throw exception when unknown file type is passed") {
    val selector = new SimpleRunnerSelector
    intercept[IllegalArgumentException] {
      selector.selectRunner(unknown)
    }
  }

}
