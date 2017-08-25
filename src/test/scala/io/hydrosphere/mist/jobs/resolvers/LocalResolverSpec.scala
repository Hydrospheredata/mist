package io.hydrosphere.mist.jobs.resolvers

import java.nio.file.Paths

import io.hydrosphere.mist.jobs.JobFile
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.apache.commons.io.FileUtils

class LocalResolverSpec extends FunSpec with Matchers with BeforeAndAfterAll {

  val basePath = "./target/data/"
  val testFileName = "existing.jar"

  override protected def beforeAll(): Unit = {
    FileUtils.touch(Paths.get(basePath, testFileName).toFile)
  }

  override protected def afterAll(): Unit = {
    FileUtils.deleteQuietly(Paths.get(basePath, testFileName).toFile)
  }

  it("should resolve file locally") {
    val resolver = new LocalResolver(s"$basePath$testFileName")
    val f = resolver.resolve()
    f.getName shouldBe testFileName
    f.exists() shouldBe true
  }

  it("should throw JobFile exception when file not found") {
    val resolver = new LocalResolver(s"${basePath}not_existsing_file.jar")
    intercept[JobFile.NotFoundException] {
      resolver.resolve()
    }
  }

}
