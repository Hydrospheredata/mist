package io.hydrosphere.mist.master.artifact

import java.io.File
import java.nio.file.{Files, Paths}

import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.master.TestUtils.AwaitSyntax
import io.hydrosphere.mist.master.data.FunctionConfigStorage
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

      val file = artifactRepo.get(testArtifactName)
      file shouldBe defined
      file.get.getName shouldBe testArtifactName
    }

    it("should not find not existing file") {
      val artifactRepo = new FsArtifactRepository(dir.toString)

      artifactRepo.get("not_existing_file.jar") should not be defined
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
    it("should list only .py and .jar files") {
      val artifactRepo = new FsArtifactRepository(dir.toString)
      val unknownExtension = Paths.get(dir.toString, "unknown.test")
      val pyExt = Paths.get(dir.toString, "test.py")
      FileUtils.touch(unknownExtension.toFile)
      FileUtils.touch(pyExt.toFile)

      artifactRepo.listPaths().await should contain allOf(testArtifactName, "test.py")

      FileUtils.deleteQuietly(pyExt.toFile)
      FileUtils.deleteQuietly(unknownExtension.toFile)
    }
  }
  describe("DefaultArtifactRepositorySpec") {

    it("should get existing default function job file path") {
      val testFilePath = Paths.get(dir.toString, testArtifactName)
      val artifactRepo = new DefaultArtifactRepository(Map(testArtifactName -> testFilePath.toFile))
      val localStorageFile = artifactRepo.get(testArtifactName)

      localStorageFile shouldBe defined
      localStorageFile.get.getName shouldBe testArtifactName
    }

    it("should not get missing default function job file path") {
      val artifactRepo = new DefaultArtifactRepository(Map())
      val testFilePath = Paths.get(dir.toString, testArtifactName)

      val localStorageFile = artifactRepo.get("not existing file")

      localStorageFile should not be defined
    }

    it("should list all files in function default storage") {
      val functions = mock[FunctionConfigStorage]
      val testFilePath = Paths.get(dir.toString, testArtifactName)
      val artifactRepo = new DefaultArtifactRepository(Map(testArtifactName -> testFilePath.toFile))

      val files = artifactRepo.listPaths().await

      files.size should equal(1)

    }

    it("should return exception when storing file") {
      val artifactRepo = new DefaultArtifactRepository(Map())
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
      val mainRepo = mock[ArtifactRepository]
      val defaultRepo = mock[ArtifactRepository]

      when(mainRepo.get(any[String]))
        .thenReturn(Some(new File("test")))
      when(defaultRepo.get(any[String]))
        .thenReturn(None)

      val artifactRepo = new SimpleArtifactRepository(mainRepo, defaultRepo)
      val file = artifactRepo.get("test")

      verify(mainRepo, times(1)).get(any[String])
      verify(defaultRepo, never).get(any[String])
      file shouldBe defined
    }
    it("should fallback on None get main repo") {
      val mainRepo = mock[ArtifactRepository]
      val defaultRepo = mock[ArtifactRepository]
      val artifactRepo = new SimpleArtifactRepository(mainRepo, defaultRepo)

      when(mainRepo.get(any[String]))
        .thenReturn(None)
      when(defaultRepo.get(any[String]))
        .thenReturn(Some(new File("test")))

      val file = artifactRepo.get("test")
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
