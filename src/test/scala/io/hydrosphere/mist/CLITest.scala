package  io.hydrosphere.mist

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.TestKit
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.cli.CLINode
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.sys.process._

//class CLITest extends WordSpecLike with BeforeAndAfterAll with Eventually {
//
//  implicit val system = ActorSystem("mist", MistConfig.Akka.CLI.settings)
//  lazy val cliActor: ActorRef = system.actorOf(Props[CLINode], name = Constants.Actors.cliName)
//
//  val timeoutAssert: Timeout = timeout(90 seconds)
//
//  object StartMist {
//    val threadMaster: Thread = {
//      new Thread {
//        override def run(): Unit = {
//          s"./bin/mist start master --config ${TestConfig.cliConfig}" !
//        }
//      }
//    }
//  }
//
//  class StartJob(route: String, externalId: String) {
//    val threadMaster: Unit = {
//      new Thread {
//        override def run(): Unit = {
//          s"bin/mist start job --config ${TestConfig.cliConfig} --route $route --external-id $externalId".!
//        }
//      }.start()
//    }
//  }
//
//  object StartJobs {
//    val routers = List(("streaming-1", "job1"), ("streaming-2", "job2"), ("streaming-3", "job3"))
//    def start(): Unit = {
//      routers.foreach {
//        router =>
//          new StartJob(router._1, router._2)
//          Thread.sleep(3000)
//      }
//    }
//  }
//
//  override def afterAll(): Unit = {
//
//    "./bin/mist stop".!
//
//    TestKit.shutdownActorSystem(system)
//
//    StartMist.threadMaster.join()
//  }
//
//  override def beforeAll(): Unit = {
//    StartMist.threadMaster.start()
//  }
//
//  def equal(a: List[Any])(b: List[Any]): Boolean = {
//    a.map(f => b.toString.contains(f.toString)).forall(_ == true)
//  }
//
//  def mockEqual(b: List[Any]): Boolean = true
//
//  def cliAsserter[A](msg: A, out: (List[Any]) => Boolean): Boolean = {
//    implicit def anyToListAny(a: Any): List[Any] = a match {
//      case list: List[Any] => list
//      case _ => List[Any](a)
//    }
//    val future = cliActor.ask(msg)(timeout = Constants.CLI.timeoutDuration)
//    val result = Await.result(future, Constants.CLI.timeoutDuration)
//    out(result)
//  }
//
//  "CLI Workers" must {
//    "list no workers" in {
//      eventually(timeoutAssert, interval(10 seconds)) {
//        assert(cliAsserter(ListWorkers(), equal(List[Any]())))
//      }
//    }
//
//    "list routers" in {
//      eventually(timeoutAssert, interval(10 seconds)) {
//        assert(cliAsserter(ListRoutes(), equal(List[Any]("streaming-1", "streaming-2", "streaming-3"))))
//      }
//    }
//
//    "start streaming and list workers" in {
//      StartJobs.start()
//
//      eventually(timeoutAssert, interval(10 seconds)) {
//        assert(cliAsserter(ListWorkers(), equal(List[Any]("streaming1", "streaming2", "streaming3"))))
//      }
//    }
//
//    "list thre jobs" in {
//      eventually(timeoutAssert, interval(10 seconds)) {
//        assert(cliAsserter(ListJobs(), equal(List[Any]("job1", "job2", "job3"))))
//      }
//    }
//
//    "list two workers after kill first" in {
//      cliAsserter(StopWorker("streaming1"), mockEqual)
//
//      eventually(timeoutAssert, interval(10 seconds)) {
//        assert(cliAsserter(ListWorkers(), equal(List[Any]("streaming2", "streaming3"))) && !cliAsserter(ListWorkers(), equal(List[Any]("streaming1"))))
//      }
//    }
//
//    "list two jobs" in {
//      eventually(timeoutAssert, interval(10 seconds)) {
//        assert(cliAsserter(ListJobs(), equal(List[Any]("job2", "job3"))) && !cliAsserter(ListJobs(), equal(List[Any]("job1"))))
//      }
//    }
//
//    "list two workers and one job after kill job" in {
//      cliAsserter(StopJob("job2"), mockEqual)
//
//      eventually(timeoutAssert, interval(10 seconds)) {
//        assert( cliAsserter(ListWorkers(), equal(List[Any]("streaming2", "streaming3")))
//          && cliAsserter(ListJobs(), equal(List[Any]("job3")))
//          && !cliAsserter(ListJobs(), equal(List[Any]("job2"))))
//      }
//    }
//
//    "list no workers after kill all" in {
//      cliAsserter(StopAllContexts(), mockEqual)
//      eventually(timeoutAssert, interval(10 seconds)) {
//        assert(cliAsserter(ListWorkers(), equal(List[Any]())))
//      }
//    }
//  }
//}
