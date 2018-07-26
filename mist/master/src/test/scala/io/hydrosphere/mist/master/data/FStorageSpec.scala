package io.hydrosphere.mist.master.data

import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigValueFactory}
import io.hydrosphere.mist.master.models.NamedConfig
import org.apache.commons.io.FileUtils
import org.scalatest._

class FStorageSpec extends FunSpec with Matchers with BeforeAndAfter {

  case class TestEntry(
    name: String,
    value: Int
  ) extends NamedConfig

  val testEntryConfigRepr = new ConfigRepr[TestEntry] {
    import scala.collection.JavaConverters._

    override def fromConfig(config: Config): TestEntry = {
      TestEntry(config.getString("name"), config.getInt("value"))
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
    val storage = FsStorage.create(path, testEntryConfigRepr)

    storage.write("one", TestEntry("one", 1))
    storage.write("two", TestEntry("two", 2))

    storage.entries should contain allOf(
      TestEntry("one", 1),
      TestEntry("two", 2)
    )

    storage.delete("one")
    storage.entries should contain allElementsOf(Seq(TestEntry("two", 2)))
  }

}
