package io.hydrosphere.mist.job

import java.io.File
import java.nio.file.Paths

import akka.actor.{Actor, ActorSystem, Status}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.core.{ExtractedFunctionData, MockitoSugar, PythonEntrySettings}
import io.hydrosphere.mist.utils.{Err, Succ}
import mist.api._
import mist.api.data._
import mist.api.encoding.JsSyntax._
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import org.mockito.Mockito.{times, verify}

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class FunctionInfoProviderActorSpec extends TestKit(ActorSystem("WorkerSpec"))
  with FunSpecLike
  with Matchers
  with MockitoSugar
  with BeforeAndAfterAll {

  val baseDir = "./target/test-fns"
  val fnPath = Paths.get(baseDir, "testfn.jar").toString

  override def beforeAll(): Unit = {
    val f = new File(baseDir)
    FileUtils.deleteQuietly(f)
    FileUtils.forceMkdir(f)
    FileUtils.writeStringToFile(new File(fnPath), "JAR CONTENT")
  }

  override def afterAll(): Unit = {
    val f = new File(baseDir)
    FileUtils.deleteQuietly(f)
  }

  val envInfo = EnvInfo(PythonEntrySettings("python", "python"))

  describe("Actor") {

    it("should get fn info when file is found") {
      val functionInfoExtractor = mock[FunctionInfoExtractor]
      when(functionInfoExtractor.extractInfo(any[File], any[String], any[EnvInfo]))
        .thenReturn(Succ(FunctionInfo(
          data = ExtractedFunctionData(
            lang = "scala",
            execute = Seq(UserInputArgument("test", MInt))
          )
        )))
      val testProbe = TestProbe()
      val fnInfoProvider = TestActorRef[Actor](FunctionInfoProviderActor.props(functionInfoExtractor))
      testProbe.send(fnInfoProvider, GetFunctionInfo("Test", fnPath, "test", envInfo))
      testProbe.expectMsg(ExtractedFunctionData(
        lang = "scala",
        execute = Seq(UserInputArgument("test", MInt)),
        name="test"
      ))
    }

    it("should delete fn from cache") {
      val functionInfoExtractor = mock[FunctionInfoExtractor]
      when(functionInfoExtractor.extractInfo(any[File], any[String], any[EnvInfo]))
        .thenReturn(Succ(FunctionInfo(
          data = ExtractedFunctionData(
            lang = "scala",
            execute = Seq(UserInputArgument("test", MInt))
          )
        )))
      val testProbe = TestProbe()
      val fnInfoProvider = TestActorRef[Actor](FunctionInfoProviderActor.props(functionInfoExtractor))
      testProbe.send(fnInfoProvider, GetFunctionInfo("Test", fnPath, "test", envInfo))
      testProbe.expectMsg(ExtractedFunctionData(
        lang = "scala",
        execute = Seq(UserInputArgument("test", MInt)),
        name="test"
      ))

    }

    it("should fail get fn info when file not found") {
      val fnInfoExtractor = mock[FunctionInfoExtractor]
      val testProbe = TestProbe()
      val fnInfoProvider = TestActorRef[Actor](FunctionInfoProviderActor.props(fnInfoExtractor))

      val notExistingFile = "not_existing_file.jar"
      testProbe.send(fnInfoProvider, GetFunctionInfo("Test", notExistingFile, "test", envInfo))

      testProbe.expectMsgType[Status.Failure]
    }

    it("should handle failure of extraction when get fn info") {
      val fnInfoExtractor = mock[FunctionInfoExtractor]
      when(fnInfoExtractor.extractInfo(any[File], any[String], any[EnvInfo]))
        .thenReturn(Err(new IllegalArgumentException("invalid fn")))

      val testProbe = TestProbe()
      val fnInfoProvider = TestActorRef[Actor](FunctionInfoProviderActor.props(fnInfoExtractor))

      testProbe.send(fnInfoProvider, GetFunctionInfo("Test", fnPath, "test", envInfo))

      testProbe.expectMsgType[Status.Failure]
    }

    it("should return validation success when validation is passed") {
      val fnInfoExtractor = mock[FunctionInfoExtractor]
      when(fnInfoExtractor.extractInfo(any[File], any[String], any[EnvInfo]))
        .thenReturn(Succ(FunctionInfo(
          TestJobInstance(Extracted.unit),
          ExtractedFunctionData()
        )))

      val testProbe = TestProbe()
      val fnInfoProviderActor = TestActorRef[Actor](FunctionInfoProviderActor.props(fnInfoExtractor))

      testProbe.send(fnInfoProviderActor, ValidateFunctionParameters(
        "Test", fnPath, "test", JsMap.empty, envInfo))
      testProbe.expectMsgType[Status.Success]

    }
    it("should return failure when file not found") {
      val fnInfoExtractor = mock[FunctionInfoExtractor]

      val testProbe = TestProbe()
      val fnInfoProviderActor = TestActorRef[Actor](FunctionInfoProviderActor.props(fnInfoExtractor))

      testProbe.send(fnInfoProviderActor, ValidateFunctionParameters(
        "Test", "not_existing_file.jar", "test", JsMap.empty, envInfo))

      testProbe.expectMsgType[Status.Failure]

    }
    it("should return failure when fail to extract instance") {
      val fnInfoExtractor = mock[FunctionInfoExtractor]
      when(fnInfoExtractor.extractInfo(any[File], any[String], any[EnvInfo]))
        .thenReturn(Err(new IllegalArgumentException("invalid")))

      val testProbe = TestProbe()
      val fnInfoProviderActor = TestActorRef[Actor](FunctionInfoProviderActor.props(fnInfoExtractor))

      testProbe.send(fnInfoProviderActor, ValidateFunctionParameters(
        "Test", fnPath, "test", JsMap.empty, envInfo))
      testProbe.expectMsgType[Status.Failure]
    }
    it("should return failure when validation fails") {
      val fnInfoExtractor = mock[FunctionInfoExtractor]
      when(fnInfoExtractor.extractInfo(any[File], any[String], any[EnvInfo]))
        .thenReturn(Succ(FunctionInfo(
          TestJobInstance(Extracted.unit),
          ExtractedFunctionData()
        )))
      when(fnInfoExtractor.extractInfo(any[File], any[String], any[EnvInfo]))
        .thenReturn(Succ(FunctionInfo(
          TestJobInstance(Failed.InternalError("invalid")),
          ExtractedFunctionData()
        )))

      val testProbe = TestProbe()
      val fnInfoProviderActor = TestActorRef[Actor](FunctionInfoProviderActor.props(fnInfoExtractor))

      testProbe.send(fnInfoProviderActor, ValidateFunctionParameters(
        "Test", fnPath, "test", JsMap.empty, envInfo))
      testProbe.expectMsgType[Status.Failure]
    }

    it("should cache fn info when requested") {
      val fnInfoExtractor = mock[FunctionInfoExtractor]
      when(fnInfoExtractor.extractInfo(any[File], any[String], any[EnvInfo]))
        .thenReturn(Succ(FunctionInfo(
          data = ExtractedFunctionData(
            lang = "scala",
            execute = Seq(UserInputArgument("test", MInt))
          )
        )))
      val testProbe = TestProbe()
      val fnInfoProviderActor = TestActorRef[Actor](FunctionInfoProviderActor.props(fnInfoExtractor))

      testProbe.send(fnInfoProviderActor, GetFunctionInfo("Test", fnPath, "test", envInfo))
      testProbe.expectMsgType[ExtractedFunctionData]
      testProbe.send(fnInfoProviderActor, GetCacheSize)
      testProbe.expectMsg(1)
    }

    it("should hit cache when get fn info") {
      val fnInfoExtractor = mock[FunctionInfoExtractor]
      when(fnInfoExtractor.extractInfo(any[File], any[String], any[EnvInfo]))
        .thenReturn(Succ(FunctionInfo(
          data = ExtractedFunctionData(
            lang = "scala",
            execute = Seq(UserInputArgument("test", MInt))
          )
        )))
      val testProbe = TestProbe()
      val fnInfoProviderActor = TestActorRef[Actor](FunctionInfoProviderActor.props(fnInfoExtractor))
      // heat up cache
      testProbe.send(fnInfoProviderActor, GetFunctionInfo("Test", fnPath, "test", envInfo))
      testProbe.expectMsgType[ExtractedFunctionData]
      testProbe.send(fnInfoProviderActor, GetFunctionInfo("Test", fnPath, "test", envInfo))
      verify(fnInfoExtractor, times(1)).extractInfo(any[File], any[String], any[EnvInfo])
    }

    it("should hit cache when validate fn info") {
      val fnInfoExtractor = mock[FunctionInfoExtractor]
      when(fnInfoExtractor.extractInfo(any[File], any[String], any[EnvInfo]))
        .thenReturn(Succ(FunctionInfo(
          instance = TestJobInstance(Extracted.unit),
          data = ExtractedFunctionData(
            lang = "scala",
            execute = Seq(UserInputArgument("test", MInt))
          )
        )))
      val testProbe = TestProbe()
      val fnInfoProviderActor = TestActorRef[Actor](FunctionInfoProviderActor.props(fnInfoExtractor))
      // heat up cache
      testProbe.send(fnInfoProviderActor, GetFunctionInfo("Test", fnPath, "test", envInfo))
      testProbe.expectMsgType[ExtractedFunctionData]
      testProbe.send(fnInfoProviderActor, ValidateFunctionParameters("Test", fnPath, "test", JsMap.empty, envInfo))
      testProbe.expectMsgType[Status.Success]

      verify(fnInfoExtractor, times(1)).extractInfo(any[File], any[String], any[EnvInfo])
    }

    it("should cache fn info when validate fn") {
      val fnInfoExtractor = mock[FunctionInfoExtractor]
      when(fnInfoExtractor.extractInfo(any[File], any[String], any[EnvInfo]))
        .thenReturn(Succ(FunctionInfo(
          instance = TestJobInstance(Extracted.unit),
          data = ExtractedFunctionData(
            lang = "scala",
            execute = Seq(UserInputArgument("test", MInt))
          )
        )))
      val testProbe = TestProbe()
      val fnInfoProviderActor = TestActorRef[Actor](FunctionInfoProviderActor.props(fnInfoExtractor))
      // heat up cache
      testProbe.send(fnInfoProviderActor, ValidateFunctionParameters("Test", fnPath, "test", JsMap.empty, envInfo))
      testProbe.expectMsgType[Status.Success]
      testProbe.send(fnInfoProviderActor, GetCacheSize)
      testProbe.expectMsg(1)
    }

    it("should get all items in request") {
      val fnInfoExtractor = mock[FunctionInfoExtractor]
      when(fnInfoExtractor.extractInfo(any[File], any[String], any[EnvInfo]))
        .thenReturn(Succ(FunctionInfo(
          instance = TestJobInstance(Extracted.unit),
          data = ExtractedFunctionData(
            lang = "scala",
            execute = Seq(UserInputArgument("test", MInt))
          )
        )))
      val testProbe = TestProbe()
      val fnInfoProviderActor = TestActorRef[Actor](FunctionInfoProviderActor.props(fnInfoExtractor))
      // heat up cache
      testProbe.send(fnInfoProviderActor,
        GetAllFunctions(List(GetFunctionInfo("Test", fnPath, "test", envInfo))))
      testProbe.expectMsg(Seq(ExtractedFunctionData(
        "test",
        "scala",
        Seq(UserInputArgument("test", MInt))
      )))
    }
  }
  describe("Cache") {
    it("should put item and create new cache with") {
      val cache = Cache(300 seconds, Map.empty[String, (Long, Int)])
      val updated = cache.put("test", 42)
      cache.size shouldBe 0
      updated.size shouldBe 1
    }
    it("should filter expired item") {
      val cache: Cache[String, Int] = Cache(0 seconds)
      val updated = cache.put("test", 42)
      val item = updated.get("test")
      item should not be defined
    }

    it("should remove expired elements on evict") {
      val cache: Cache[String, Int] = Cache(0 seconds)
      val updated = cache.put("test", 42)
      updated.size shouldBe 1
      val evicted = updated.evictAll
      evicted.size shouldBe 0
    }
  }

  def TestJobInstance(validationResult: Extraction[Unit]): BaseFunctionInstance = new BaseFunctionInstance {
    override def run(jobCtx: FullFnContext): Either[Throwable, JsData] = Right(JsNull)

    override def validateParams(params: JsMap): Extraction[Unit] = validationResult

    override def describe(): Seq[ArgInfo] = Seq.empty

    override def lang: String = "scala"
  }
}
