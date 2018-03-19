package io.hydrosphere.mist.master.data

import java.nio.file.Paths

import io.hydrosphere.mist.master.models.FunctionConfig
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, Matchers, FunSpec}

class FunctionConfigStorageSpec extends FunSpec with Matchers with BeforeAndAfter {

  val path = "./target/data/func_store_test"

  before {
    val f = Paths.get(path).toFile
    if (f.exists()) FileUtils.deleteDirectory(f)
  }

  import scala.concurrent.ExecutionContext.Implicits.global
  import io.hydrosphere.mist.master.TestUtils._

  it("should update") {
    val functions = testStorage()

    functions.all.await.size shouldBe 1

    functions.update(FunctionConfig("second", "path", "className", "foo")).await
    functions.all.await.size shouldBe 2
  }

  it("should get") {
    val functions = testStorage()

    functions.get("first").await.isDefined shouldBe true
    functions.get("second").await.isDefined shouldBe false

    functions.update(FunctionConfig("second", "path", "className", "foo")).await
    functions.get("second").await.isDefined shouldBe true
  }

  it("should override defaults") {
    val functions = testStorage()

    functions.get("first").await.get.className shouldBe "className"

    functions.update(FunctionConfig("first", "path", "anotherClassName", "foo")).await
    functions.get("first").await.get.className shouldBe "anotherClassName"
  }

  def testStorage(
    defaults: Seq[FunctionConfig] = Seq(FunctionConfig("first", "path", "className", "foo"))): FunctionConfigStorage = {
    new FunctionConfigStorage(
      FsStorage.create(path, ConfigRepr.EndpointsRepr),
      defaults
    )
  }
}
