package io.hydrosphere.mist.worker

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestActorRef, TestActor, TestKit, TestProbe}
import io.hydrosphere.mist.Messages.JobMessages._
import io.hydrosphere.mist.jobs.Action
import io.hydrosphere.mist.worker.runners.JobRunner
import org.apache.spark.SparkConf
import org.scalatest._

import scala.concurrent.duration.Duration

class WorkerActorSpec extends TestKit(ActorSystem("WorkerSpec"))
  with FunSpecLike
  with Matchers
  with BeforeAndAfterAll {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("test")
    .set("spark.driver.allowMultipleContexts", "true")

  var context: NamedContext = _

  override def afterAll {
    TestKit.shutdownActorSystem(system)
    context.stop()
  }

  override def beforeAll {
    context = NamedContext("test", conf)
  }

  it("should execute jobs") {
    val runner = SuccessRunner(Map("answer" -> 42))

    val probe = TestProbe()
    val worker = testWorkerActor(runner)

    probe.send(worker, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))

    probe.expectMsgType[JobStarted]
    probe.expectMsgPF(){
      case JobSuccess("id", r) =>
        r shouldBe Map("answer" -> 42)
    }
  }

  it("should respond failure") {
    val runner = FailureRunner("Expected error")
    val worker = testWorkerActor(runner)

    val probe = TestProbe()
    probe.send(worker, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))

    probe.expectMsgType[JobStarted]
    probe.expectMsgPF(){
      case JobFailure("id", e) =>
        e shouldBe "Expected error"
    }
  }

  it("should limit jobs") {
    val runner = SuccessRunner(Map("yoyo" -> "hey"))

    val probe = TestProbe()
    val worker = testWorkerActor(runner, 2)

    probe.send(worker, RunJobRequest("1", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))
    probe.send(worker, RunJobRequest("2", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))
    probe.send(worker, RunJobRequest("3", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))

    probe.expectMsgType[JobStarted]
    probe.expectMsgType[JobStarted]
    probe.expectMsgType[WorkerIsBusy]
  }

  private def testWorkerActor(runner: JobRunner, maxJobs: Int = 10): ActorRef = {
    val props = Props(
      classOf[WorkerActor],
      "test", context, runner, Duration.Inf, 2)
    //val ref = system.actorOf(props)

    TestActorRef[WorkerActor](props)
  }

  it("should cancel job") {
    val runner = new JobRunner {
      override def run(p: JobParams, c: NamedContext): Either[String, Map[String, Any]] = {
        val sc = c.context
        val r = sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10000); i }.count()
        Right(Map("r" -> "Ok"))

      }
    }

    val worker = testWorkerActor(runner)

    val probe = TestProbe()
    probe.send(worker, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))
    probe.send(worker, CancelJobRequest("id"))

    probe.expectMsgType[JobStarted]
    probe.expectMsgType[JobIsCancelled]
  }

  def SuccessRunner(r: Map[String, Any]): JobRunner =
    testRunner(Right(r))

  def FailureRunner(error: String): JobRunner =
    testRunner(Left(error))

  def testRunner(f: => Either[String, Map[String, Any]]): JobRunner = {
    new JobRunner {
      def run(p: JobParams, c: NamedContext): Either[String, Map[String, Any]] = f
    }
  }
}

