package io.hydrosphere.mist.worker

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.worker.runners.{ArtifactDownloader, JobRunner, RunnerSelector}
import mist.api.data.{JsData, _}
import mist.api.encoding.defaultEncoders._
import mist.api.encoding.JsSyntax._
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

class WorkerActorSpec extends TestKit(ActorSystem("WorkerSpec"))
  with FunSpecLike
  with Matchers
  with MockitoSugar
  with BeforeAndAfterAll {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("test")
    .set("spark.driver.allowMultipleContexts", "true")

  var context: MistScContext = _
  var spContext: SparkContext = _

  val artifactDownloader = {
    val default = mock[ArtifactDownloader]
    when(default.downloadArtifact(any[String]))
      .thenSuccess(SparkArtifact(new File("doesn't matter"), "url"))
    default
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
    context.stop()
    spContext.stop()
  }

  override def beforeAll {
    spContext = new SparkContext(conf)
    context = new MistScContext(spContext, "test") {
      override def stop(): Unit = {} //do not close ctx during tests
    }
  }


  it(s"should execute jobs") {
    val runner = SuccessRunnerSelector(JsNumber(42))
    val worker = createActor(runner)
    val probe = TestProbe()

    probe.send(worker, RunJobRequest("id", JobParams("path", "MyClass", JsMap.empty, action = Action.Execute)))

    probe.expectMsgType[JobFileDownloading]
    probe.expectMsgType[JobStarted]
    probe.expectMsgPF(){
      case JobSuccess("id", r) =>
        r shouldBe JsNumber(42)
    }
  }

  it(s"should respond failure") {
    val runner = FailureRunnerSelector("Expected error")
    val worker = createActor(runner)

    val probe = TestProbe()
    probe.send(worker, RunJobRequest("id", JobParams("path", "MyClass", JsMap.empty, action = Action.Execute)))
    probe.expectMsgType[JobFileDownloading]
    probe.expectMsgType[JobStarted]
    probe.expectMsgPF() {
      case JobFailure("id", e) =>
        e should not be empty
    }
  }

  it(s"should cancel job") {
    val runnerSelector = RunnerSelector(new JobRunner {
      override def run(req: RunJobRequest, c: MistScContext): Either[Throwable, JsData] = {
        val sc = c.sc
        val r = sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10000); i }.count()
        Right(JsMap("r" -> JsString("Ok")))
      }
    })

    val worker = createActor(runnerSelector)

    val probe = TestProbe()
    probe.send(worker, RunJobRequest("id", JobParams("path", "MyClass", JsMap.empty, action = Action.Execute)))
    probe.send(worker, CancelJobRequest("id"))

    probe.expectMsgAllConformingOf(classOf[JobFileDownloading], classOf[JobStarted], classOf[JobIsCancelled])
  }

  def createActor(runnerSelector: RunnerSelector): TestActorRef[WorkerActor] = {
    val props  = WorkerActor.props(context, artifactDownloader, RequestSetup.NOOP, runnerSelector)
    TestActorRef[WorkerActor](props)
  }

  it("should limit jobs") {
    val runnerSelector = SuccessRunnerSelector({
      Thread.sleep(1000)
      JsMap("yoyo" -> JsString("hey"))
    })

    val probe = TestProbe()
    val worker = createActor(runnerSelector)

    probe.send(worker, RunJobRequest("1", JobParams("path", "MyClass", JsMap.empty, action = Action.Execute)))
    probe.send(worker, RunJobRequest("2", JobParams("path", "MyClass", JsMap.empty, action = Action.Execute)))
    probe.send(worker, RunJobRequest("3", JobParams("path", "MyClass", JsMap.empty, action = Action.Execute)))

    probe.expectMsgAllConformingOf(
      classOf[JobFileDownloading],
      classOf[JobStarted],
      classOf[WorkerIsBusy],
      classOf[WorkerIsBusy]
    )
  }

  def RunnerSelector(r: JobRunner): RunnerSelector =
    new RunnerSelector {
      override def selectRunner(artifact: SparkArtifact): JobRunner = r
    }

  def SuccessRunnerSelector(r: => JsData): RunnerSelector =
    new RunnerSelector {
      override def selectRunner(artifact: SparkArtifact): JobRunner = SuccessRunner(r)
    }

  def FailureRunnerSelector(error: String): RunnerSelector =
    new RunnerSelector {
      override def selectRunner(artifact: SparkArtifact): JobRunner = FailureRunner(error)
    }

  def SuccessRunner(r: => JsData): JobRunner =
    testRunner(Right(r))

  def FailureRunner(error: String): JobRunner =
    testRunner(Left(new RuntimeException(error)))

  def testRunner(f: => Either[Throwable, JsData]): JobRunner = {
    new JobRunner {
      def run(p: RunJobRequest, c: MistScContext): Either[Throwable, JsData] = f
    }
  }
}

