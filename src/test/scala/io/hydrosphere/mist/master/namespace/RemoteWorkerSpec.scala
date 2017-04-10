package io.hydrosphere.mist.master.namespace

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import io.hydrosphere.mist.contexts.NamedContext
import io.hydrosphere.mist.jobs.Action
import io.hydrosphere.mist.master.namespace.WorkerActor._
import org.apache.spark.SparkConf
import org.scalatest._

class RemoteWorkerSpec extends TestKit(ActorSystem("WorkerSpec"))
  with FunSpecLike
  with ImplicitSender
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

    val worker = system.actorOf(
      Props(classOf[WorkerActor], "test", context, runner, 10)
    )

    worker ! RunJobRequest("id", JobParams("path", "MyClass", Map.empty, action = Action.Execute))

    expectMsgType[JobStarted]
    expectMsgPF(){
      case JobSuccess("id", r) =>
        r shouldBe Map("answer" -> 42)
    }
  }

  it("should respond failure") {
    val runner = FailureRunner("Expected error")
    val worker = system.actorOf(
      Props(classOf[WorkerActor], "test", context, runner, 10)
    )

    worker ! RunJobRequest("id", JobParams("path", "MyClass", Map.empty, action = Action.Execute))

    expectMsgType[JobStarted]
    expectMsgPF(){
      case JobFailure("id", e) =>
        e shouldBe "Expected error"
    }
  }

  it("should limit jobs") {
    val runner = SuccessRunner(Map("yoyo" -> "hey"))

    val worker = system.actorOf(
      Props(classOf[WorkerActor], "test", context, runner, 2)
    )

    worker ! RunJobRequest("1", JobParams("path", "MyClass", Map.empty, action = Action.Execute))
    worker ! RunJobRequest("2", JobParams("path", "MyClass", Map.empty, action = Action.Execute))
    worker ! RunJobRequest("3", JobParams("path", "MyClass", Map.empty, action = Action.Execute))

    expectMsgType[JobStarted]
    expectMsgType[JobStarted]
    expectMsgType[WorkerIsBusy]
  }

//  it("should cancel job") {
//    val runner = new JobRunner {
//      override def run(p: JobParams, c: NamedContext): Either[Throwable, Map[String, Any]] = {
//        println("IN RUN", Thread.currentThread().getName)
//        val sc = c.context
//        val r = sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(100); i }.count()
//        println("DONE")
//        Right(Map("r" -> r))
//      }
//    }
//
//    val realSystem = ActorSystem("TestConcurrentCase")
//    val worker = realSystem.actorOf(
//      Props(classOf[RemoteWorker], "test", context, runner, 2)
//    )
//
//    println(Thread.currentThread().getName)
//
//    worker ! RunJobRequest("id", JobParams("path", "MyClass", Map.empty, action = Action.Execute))
//    worker ! CancelJobRequest("id")
//
//    expectMsgType[JobStarted]
//    expectMsgType[JobIsCancelled]
//    expectMsgType[JobStarted]
//
//    realSystem.shutdown()
//  }

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
