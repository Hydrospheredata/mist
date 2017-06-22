package io.hydrosphere.mist.master.logging

import java.nio.file.{Files, Paths}

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{TestActorRef, TestKit}
import akka.util.Timeout
import io.hydrosphere.mist.api.logging.MistLogging.LogEvent
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class LogWriterSpec extends TestKit(ActorSystem("log-writer-test"))
  with FunSpecLike
  with Matchers
  with BeforeAndAfterAll {

  val dirName = "log_writer_test"
  val dir = Paths.get(".", "target", dirName)

  override def beforeAll(): Unit = {
    Files.createDirectories(dir)
  }

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(dir.toFile)
  }

  implicit val timeout = Timeout(5 second)

  describe("writer actor") {

    it("should write to file") {
      val path = dir.resolve("test")
      val f = path.toFile
      if (f.exists()) f.delete()
      Files.createFile(path)

      val actor = TestActorRef(WriterActor.props(path))

      val event = LogEvent.mkDebug("id", "message")
      val future = actor ? WriteRequest("id", Seq(event))
      val update = Await.result(future.mapTo[LogUpdate], Duration.Inf)

      update.jobId shouldBe "id"
      update.events shouldBe Seq(event)
      update.bytesOffset shouldBe (event.mkString + "\n").getBytes.length
    }
  }

  describe("writers group") {

    it("should proxy to writer") {
      val mappings = new LogStorageMappings(dir)
      val expectedPath = mappings.pathFor("id")
      if (Files.exists(expectedPath)) Files.delete(expectedPath)

      val actor = TestActorRef(WritersGroupActor.props(mappings))

      val event = LogEvent.mkDebug("id", "message")
      val future = actor ? WriteRequest("id", Seq(event))
      val update = Await.result(future.mapTo[LogUpdate], Duration.Inf)

      val expectedSize = (event.mkString + "\n").getBytes.length

      update.jobId shouldBe "id"
      update.events shouldBe Seq(event)
      update.bytesOffset shouldBe expectedSize

      Files.readAllBytes(expectedPath).length shouldBe expectedSize
    }
  }
}
