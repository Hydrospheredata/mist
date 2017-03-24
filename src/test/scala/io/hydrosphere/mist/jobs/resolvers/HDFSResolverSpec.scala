package io.hydrosphere.mist.jobs.resolvers

import java.io.File
import java.net.URI
import java.nio.file.{Paths, Files}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path => HPath}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.scalatest.{BeforeAndAfterAll, Matchers, FunSpec}

class HDFSResolverSpec extends FunSpec with Matchers with BeforeAndAfterAll {

  var cluster: MiniDFSCluster = _

  override def beforeAll = {
    cluster = createCluster()
  }

  override def afterAll = {
    cluster.shutdown()
  }

  it("should copy file from hdfs") {
    val port = cluster.getNameNodePort
    val uri = s"hdfs://localhost:$port"

    val content = "JAR CONTENT"
    writeToFile(uri, "test.jar", content)

    val filePath = s"$uri/test.jar"
    val resolver = new HDFSResolver(filePath, "target")
    resolver.exists shouldBe true

    val file = resolver.resolve
    val bytes = Files.readAllBytes(Paths.get(file.getPath))

    new String(bytes) shouldBe content
  }

  def createCluster(): MiniDFSCluster = {
    val baseDir = new File("./target/hdfs/").getAbsoluteFile
    FileUtil.fullyDelete(baseDir)
    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    val builder = new MiniDFSCluster.Builder(conf)
    builder.build()
  }

  def writeToFile(uri: String, name: String, content: String): Unit = {
    val fs = FileSystem.get(new URI(uri), new Configuration())
    val stream = fs.create(new HPath(s"$uri/$name"))
    stream.write(content.getBytes)
    stream.close()
  }

}
