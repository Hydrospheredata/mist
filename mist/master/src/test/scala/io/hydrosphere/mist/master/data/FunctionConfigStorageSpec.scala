package io.hydrosphere.mist.master.data

import java.nio.file.Paths

import io.hydrosphere.mist.master.models.FunctionConfig
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, Matchers, FunSpec}

class FunctionConfigStorageSpec extends FunSpec with Matchers with BeforeAndAfter {

  val path = "./target/data/endps_store_test"

  before {
    val f = Paths.get(path).toFile
    if (f.exists()) FileUtils.deleteDirectory(f)
  }

  import scala.concurrent.ExecutionContext.Implicits.global
  import io.hydrosphere.mist.master.TestUtils._

  it("should update") {
    val endpoints = testStorage()

    endpoints.all.await.size shouldBe 1

    endpoints.update(FunctionConfig("second", "path", "className", "foo")).await
    endpoints.all.await.size shouldBe 2
  }

  it("should get") {
    val endpoints = testStorage()

    endpoints.get("first").await.isDefined shouldBe true
    endpoints.get("second").await.isDefined shouldBe false

    endpoints.update(FunctionConfig("second", "path", "className", "foo")).await
    endpoints.get("second").await.isDefined shouldBe true
  }

  it("should override defaults") {
    val endpoints = testStorage()

    endpoints.get("first").await.get.className shouldBe "className"

    endpoints.update(FunctionConfig("first", "path", "anotherClassName", "foo")).await
    endpoints.get("first").await.get.className shouldBe "anotherClassName"
  }

  def testStorage(
    defaults: Seq[FunctionConfig] = Seq(FunctionConfig("first", "path", "className", "foo"))): FunctionConfigStorage = {
    new FunctionConfigStorage(
      FsStorage.create(path, ConfigRepr.EndpointsRepr),
      defaults
    )
  }
}
