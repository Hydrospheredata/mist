package io.hydrosphere.mist.master.data

import java.nio.file.Paths

import io.hydrosphere.mist.master.{TestData, ContextsSettings}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, Matchers, FunSpec}

class ContextsStorageSpec extends FunSpec with Matchers with BeforeAndAfter {

  val path = "./target/data/ctx_store_test"

  before {
    val f = Paths.get(path).toFile
    if (f.exists()) FileUtils.deleteDirectory(f)
  }

  it("should use defaults") {
    val contexts = new ContextsStorage(FsStorage.create(path, ConfigRepr.ContextConfigRepr), TestData.contextSettings)

    println(contexts.all)
  }
}
