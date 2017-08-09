package io.hydrosphere.mist.master.artifact

import java.io.File
import java.nio.file.{Files, Paths}

import io.hydrosphere.mist.MockitoSugar
import io.hydrosphere.mist.master.TestUtils.AwaitSyntax
import io.hydrosphere.mist.master.data.EndpointsStorage
import io.hydrosphere.mist.master.models.EndpointConfig
import org.apache.commons.io.FileUtils
import org.mockito.Mockito.{never, times, verify}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

class ArtifactRepositorySpec extends FunSpecLike
  with Matchers
  with MockitoSugar
  with BeforeAndAfterAll {

  val dirName = "artifact_test"
  val dir = Paths.get(".", "target", dirName)
  val testArtifactName = "testArtifact.jar"

  override protected def beforeAll(): Unit = {
    Files.createDirectories(dir)
    Files.createFile(Paths.get(dir.toString, testArtifactName))
  }

  override protected def afterAll(): Unit = {
    FileUtils.deleteDirectory(dir.toFile)
  }

  describe("FsArtifactRepository") {
    it("should list all stored file names") {
      val artifactRepo = new FsArtifactRepository(dir.toString)
      val files = artifactRepo.listPaths().await
      files.size shouldBe 1
      files.foreach {
        _ shouldBe testArtifactName
      }
    }

    it("should get file by name") {
      val artifactRepo = new FsArtifactRepository(dir.toString)

      val file = artifactRepo.get(testArtifactName).await
      file shouldBe defined
      file.get.getName shouldBe testArtifactName
    }

    it("should not find not existing file") {
      val artifactRepo = new FsArtifactRepository(dir.toString)

      artifactRepo.get("not_existing_file.jar").await should not be defined
    }

    it("should store file") {
      val artifactRepo = new FsArtifactRepository(dir.toString)
      val testFilename = "someFile.jar"
      val source = Files.createFile(Paths.get(dir.toString, "temp_file")).toFile

      val file = artifactRepo.store(source, testFilename).await

      file.exists() shouldBe true
      file.getParent shouldBe dir.toString
      file.getName shouldBe testFilename

      file.delete()
      source.delete()
    }
  }
  describe("DefaultArtifactRepositorySpec") {

    it("should get existing default endpoint job file path") {
      val endpointStorage = mock[EndpointsStorage]
      val artifactRepo = new DefaultArtifactRepository(endpointStorage)
      val testFilePath = Paths.get(dir.toString, testArtifactName)
      val str = testFilePath.toString
      println(str.substring(str.lastIndexOf('/') + 1, str.length))
      when(endpointStorage.all)
        .thenReturn(Future.successful(Seq(EndpointConfig("test", testFilePath.toString, "Test", "test"))))

      val localStorageFile = artifactRepo.get(testArtifactName).await

      localStorageFile shouldBe defined
      localStorageFile.get.getName shouldBe testArtifactName
    }

    it("should not get missing default endpoint job file path") {
      val endpointStorage = mock[EndpointsStorage]
      val artifactRepo = new DefaultArtifactRepository(endpointStorage)
      val testFilePath = Paths.get(dir.toString, testArtifactName)

      when(endpointStorage.all)
        .thenReturn(Future.successful(Seq()))

      val localStorageFile = artifactRepo.get("not existing file").await

      localStorageFile should not be defined
    }

    it("should list all files in endpoint default storage") {
      val endpointStorage = mock[EndpointsStorage]
      val artifactRepo = new DefaultArtifactRepository(endpointStorage)
      val testFilePath = Paths.get(dir.toString, testArtifactName)

      when(endpointStorage.all)
        .thenReturn(Future.successful(Seq(
          EndpointConfig("test", testFilePath.toString, "Test", "test")
        )))

      val files = artifactRepo.listPaths().await

      files.size should equal(1)

    }
    it("should get full path of hdfs or mvn artifact") {
      val endpointStorage = mock[EndpointsStorage]
      val artifactRepo = new DefaultArtifactRepository(endpointStorage)
      val testFilePath = Paths.get(dir.toString, testArtifactName)

      val mvnPath = "mvn://http://localhost:8081/artifactory/releases :: io.hydrosphere % mist_2.10 % 0.0.1"
      val hdfsPath = "hdfs://localhost:0/test.jar"

      when(endpointStorage.all)
        .thenReturn(Future.successful(Seq(
          EndpointConfig("test", testFilePath.toString, "Test", "test"),
          EndpointConfig("mvn", mvnPath, "test", "test"),
          EndpointConfig("hdfs", hdfsPath, "Test", "Test")
        )))

      val paths = artifactRepo.listPaths().await

      paths.size should equal(3)
      paths shouldBe Set(testArtifactName, mvnPath, hdfsPath)
    }

    it("should return exception when storing file") {
      val endpointStorage = mock[EndpointsStorage]
      val artifactRepo = new DefaultArtifactRepository(endpointStorage)

      val source = Files.createFile(Paths.get(dir.toString, "temp_file")).toFile


      val res = artifactRepo.store(source, "test.jar")
      ScalaFutures.whenReady(res.failed) { ex =>
        ex shouldBe an[UnsupportedOperationException]
        source.delete()
      }
    }
  }
  describe("SimpleArtifactRepository") {
    it("should call get on main repo") {
      val mainRepo = mock[FsArtifactRepository]
      val defaultRepo = mock[ArtifactRepository]

      when(mainRepo.get(any[String]))
        .thenReturn(Future.successful(Some(new File("test"))))
      when(defaultRepo.get(any[String]))
        .thenReturn(Future.successful(None))

      val artifactRepo = new SimpleArtifactRepository(mainRepo, defaultRepo)

      val file = artifactRepo.get("test").await
      verify(mainRepo, times(1)).get(any[String])
      verify(defaultRepo, times(1)).get(any[String])
      file shouldBe defined
    }
    it("should fallback on None get main repo") {
      val mainRepo = mock[ArtifactRepository]
      val defaultRepo = mock[ArtifactRepository]
      val artifactRepo = new SimpleArtifactRepository(mainRepo, defaultRepo)

      when(mainRepo.get(any[String]))
        .thenReturn(Future.successful(None))
      when(defaultRepo.get(any[String]))
        .thenReturn(Future.successful(Some(new File("test"))))

      val file = artifactRepo.get("test").await
      verify(mainRepo, times(1)).get(any[String])
      verify(defaultRepo, times(1)).get(any[String])
      file shouldBe defined
    }
    it("should combine 2 repos files") {
      val mainRepo = mock[ArtifactRepository]
      val defaultRepo = mock[ArtifactRepository]
      val artifactRepo = new SimpleArtifactRepository(mainRepo, defaultRepo)

      when(mainRepo.listPaths())
        .thenReturn(Future.successful(Set("test1")))
      when(defaultRepo.listPaths())
        .thenReturn(Future.successful(Set("test")))

      val files = artifactRepo.listPaths().await
      verify(mainRepo, times(1)).listPaths()
      verify(defaultRepo, times(1)).listPaths()
      files.size shouldBe 2
    }
    it("should only store on main repo") {
      val mainRepo = mock[ArtifactRepository]
      val defaultRepo = mock[ArtifactRepository]
      val artifactRepo = new SimpleArtifactRepository(mainRepo, defaultRepo)
      val source = Files.createFile(Paths.get(dir.toString, "temp_file")).toFile

      when(mainRepo.store(any[File], any[String]))
        .thenReturn(Future.successful(Paths.get(dir.toString, testArtifactName).toFile))

      val res = artifactRepo.store(source, testArtifactName)

      verify(mainRepo, times(1)).store(any[File], any[String])
      verify(defaultRepo, never()).store(any[File], any[String])

      res.await

      source.delete()
    }
  }

}
