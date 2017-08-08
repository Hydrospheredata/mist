package io.hydrosphere.mist.master.artifact

import java.io.File
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.hydrosphere.mist.MockitoSugar
import io.hydrosphere.mist.master.TestUtils.AwaitSyntax
import io.hydrosphere.mist.master.data.EndpointsStorage
import io.hydrosphere.mist.master.models.EndpointConfig
import org.apache.commons.io.FileUtils
import org.mockito.Mockito.{never, times, verify}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits._

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
      val files = artifactRepo.loadAll().await
      files.size shouldBe 1
      files.foreach {
        _.getName shouldBe testArtifactName
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

      val source = Source.single(ByteString.fromString("some content"))

      val testFilename = "someFile.jar"
      val file = artifactRepo.store(source, testFilename).await

      FileUtils.readFileToString(file, Charset.defaultCharset()) shouldBe "some content"

    }
  }
  describe("DefaultArtifactRepositorySpec") {

    it("should get existing default endpoint job file path") {
      val endpointStorage = mock[EndpointsStorage]
      val artifactRepo = new DefaultArtifactRepository(endpointStorage)
      val testFilePath = Paths.get(dir.toString, testArtifactName)
      when(endpointStorage.validatePath(any[String]))
        .thenReturn(Right(EndpointConfig("test", testFilePath.toString, "Test", "test")))

      val localStorageFile = artifactRepo.get(testArtifactName).await

      localStorageFile shouldBe defined
      localStorageFile.get.getName shouldBe testArtifactName
    }

    it("should not get missing default endpoint job file path") {
      val endpointStorage = mock[EndpointsStorage]
      val artifactRepo = new DefaultArtifactRepository(endpointStorage)
      val testFilePath = Paths.get(dir.toString, testArtifactName)

      when(endpointStorage.validatePath(any[String]))
        .thenReturn(Left(new IllegalArgumentException("not found")))

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

      val files = artifactRepo.loadAll().await

      files.size should equal(1)

    }

    it("should return exception when storing file") {
      val endpointStorage = mock[EndpointsStorage]
      val artifactRepo = new DefaultArtifactRepository(endpointStorage)
      val res = artifactRepo.store(Source.single(ByteString.fromString("test")), "test.jar")
      ScalaFutures.whenReady(res.failed) { ex =>
        ex shouldBe an[UnsupportedOperationException]
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

      when(mainRepo.loadAll())
        .thenReturn(Future.successful(Seq(new File("test1"))))
      when(defaultRepo.loadAll())
        .thenReturn(Future.successful(Seq(new File("test"))))

      val files = artifactRepo.loadAll().await
      verify(mainRepo, times(1)).loadAll()
      verify(defaultRepo, times(1)).loadAll()
      files.size shouldBe 2
    }
    it("should only store on main repo") {
      val mainRepo = mock[ArtifactRepository]
      val defaultRepo = mock[ArtifactRepository]
      val artifactRepo = new SimpleArtifactRepository(mainRepo, defaultRepo)

      when(mainRepo.store(any[Source[ByteString, Any]], any[String]))
        .thenReturn(Future.successful())

      val res = artifactRepo.store(Source.single(ByteString.fromString("test")), "test.py")

      verify(mainRepo, times(1)).store(any[Source[ByteString, Any]], any[String])
      verify(defaultRepo, never()).store(any[Source[ByteString, Any]], any[String])

      res.await
    }
  }

}
