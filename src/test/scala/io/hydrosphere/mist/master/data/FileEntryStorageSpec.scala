package io.hydrosphere.mist.master.data

import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.commons.io.FileUtils
import org.scalatest._

class FileEntryStorageSpec
  extends FunSpec with Matchers with BeforeAndAfter {

  case class TestEntry(
    name: String,
    value: Int
  )

  implicit val testEntryConfigRepr = new ConfigRepr[TestEntry] {
    import scala.collection.JavaConverters._

    override def name(a: TestEntry): String = a.name

    override def fromConfig(name: String, config: Config): TestEntry = {
      TestEntry(name, config.getInt("value"))
    }

    override def toConfig(a: TestEntry): Config = {
      val map = Map("value" -> ConfigValueFactory.fromAnyRef(a.value))
      ConfigValueFactory.fromMap(map.asJava).toConfig
    }
  }

  val path = "./target/file_store_test"

  before {
    val f = Paths.get(path).toFile
    if (f.exists()) FileUtils.deleteDirectory(f)
  }

  it("should store files") {
    val storage = FsStorage.create[TestEntry](path)

    storage.write(TestEntry("one", 1))
    storage.write(TestEntry("two", 2))

    storage.entries should contain allOf(
      TestEntry("one", 1),
      TestEntry("two", 2)
    )
  }

}
