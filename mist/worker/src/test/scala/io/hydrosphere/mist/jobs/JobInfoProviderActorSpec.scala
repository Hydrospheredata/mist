package io.hydrosphere.mist.jobs

import java.io.File
import java.nio.file.Paths

import akka.actor.{Actor, ActorSystem, Status}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import io.hydrosphere.mist.core.CommonData.{Action, GetJobInfo, ValidateJobParameters}
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.core.jvmjob.FullJobInfo
import io.hydrosphere.mist.job.{JobInfo, JobInfoExtractor, JobInfoProviderActor}
import mist.api.args.{ArgInfo, UserInputArgument}
import mist.api.FullJobContext
import mist.api.args.MInt
import mist.api.data.{JsLikeData, JsLikeNull}
import mist.api.internal.BaseJobInstance
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.util.{Failure, Success}

class JobInfoProviderActorSpec extends TestKit(ActorSystem("WorkerSpec"))
  with FunSpecLike
  with Matchers
  with MockitoSugar
  with BeforeAndAfterAll {

  val baseDir = "./target/test-jobs"
  val jobPath = Paths.get(baseDir, "testJob.jar").toString

  override def beforeAll(): Unit = {
    val f = new File(baseDir)
    FileUtils.deleteQuietly(f)
    FileUtils.forceMkdir(f)
    FileUtils.touch(new File(jobPath))
  }

  override def afterAll(): Unit = {
    val f = new File(baseDir)
    FileUtils.deleteQuietly(f)
  }


  it("should get job info when file is found") {
    val jobInfoExtractor = mock[JobInfoExtractor]
    when(jobInfoExtractor.extractInfo(any[File], any[String]))
      .thenReturn(Success(JobInfo(
        info=FullJobInfo(
          lang="scala",
          execute=Seq(UserInputArgument("test", MInt)),
          className="Test"
        )
      )))
    val testProbe = TestProbe()
    val jobInfoProvider = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))
    testProbe.send(jobInfoProvider, GetJobInfo("Test", jobPath))
    testProbe.expectMsg(FullJobInfo(
      lang="scala",
      execute=Seq(UserInputArgument("test", MInt)),
      className="Test"
    ))
  }

  it("should fail get job info when file not found") {
    val jobInfoExtractor = mock[JobInfoExtractor]
    val testProbe = TestProbe()
    val jobInfoProvider = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))

    val notExistingFile = "not_existing_file.jar"
    testProbe.send(jobInfoProvider, GetJobInfo("Test", notExistingFile))

    testProbe.expectMsgType[Status.Failure]
  }

  it("should handle failure of extraction when get job info") {
    val jobInfoExtractor = mock[JobInfoExtractor]
    when(jobInfoExtractor.extractInfo(any[File], any[String]))
      .thenReturn(Failure(new IllegalArgumentException("invalid job")))

    val testProbe = TestProbe()
    val jobInfoProvider = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))

    testProbe.send(jobInfoProvider, GetJobInfo("Test", jobPath))

    testProbe.expectMsgType[Status.Failure]
  }

  it("should return validation success when validation is passed") {
    val jobInfoExtractor = mock[JobInfoExtractor]
    when(jobInfoExtractor.extractInstance(any[File], any[String], any[Action]))
      .thenReturn(Success(TestJobInstance(Right(Map.empty))))

    val testProbe = TestProbe()
    val jobInfoProviderActor = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))

    testProbe.send(jobInfoProviderActor, ValidateJobParameters("Test", jobPath, Action.Execute, Map.empty))
    testProbe.expectMsgType[Status.Success]

  }
  it("should return failure when file not found") {
    val jobInfoExtractor = mock[JobInfoExtractor]

    val testProbe = TestProbe()
    val jobInfoProviderActor = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))

    testProbe.send(jobInfoProviderActor, ValidateJobParameters("Test", "not_existing_file.jar", Action.Execute, Map.empty))

    testProbe.expectMsgType[Status.Failure]

  }
  it("should return failure when fail to extract instance") {
    val jobInfoExtractor = mock[JobInfoExtractor]
    when(jobInfoExtractor.extractInstance(any[File], any[String], any[Action]))
      .thenReturn(Failure(new IllegalArgumentException("invalid")))

    val testProbe = TestProbe()
    val jobInfoProviderActor = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))

    testProbe.send(jobInfoProviderActor, ValidateJobParameters("Test", jobPath, Action.Execute, Map.empty))
    testProbe.expectMsgType[Status.Failure]
  }
  it("should return failure when validation fails") {
    val jobInfoExtractor = mock[JobInfoExtractor]
    when(jobInfoExtractor.extractInstance(any[File], any[String], any[Action]))
      .thenReturn(Success(TestJobInstance(
        Left(new IllegalArgumentException("invalid"))
      )))

    val testProbe = TestProbe()
    val jobInfoProviderActor = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))

    testProbe.send(jobInfoProviderActor, ValidateJobParameters("Test", jobPath, Action.Execute, Map.empty))
    testProbe.expectMsgType[Status.Failure]
  }

  def TestJobInstance(validationResult: Either[Throwable, Map[String, Any]]): BaseJobInstance = new BaseJobInstance {
    override def run(jobCtx: FullJobContext): Either[Throwable, JsLikeData] = Right(JsLikeNull)

    override def validateParams(params: Map[String, Any]): Either[Throwable, Any] = validationResult

    override def describe(): Seq[ArgInfo] = Seq.empty
  }
}
