package  io.hydrosphere.mist

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.TestKit
import akka.util.Timeout
import io.hydrosphere.mist.Messages.{ListMessage, StopAllContexts, StringMessage}
import io.hydrosphere.mist.worker.CLINode
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.sys.process._

class CLITest extends WordSpecLike with BeforeAndAfterAll with Eventually {

  implicit val system = ActorSystem("mist", MistConfig.Akka.CLI.settings)
  val cliActor = system.actorOf(Props[CLINode], name = Constants.CLI.cliActorName )
  implicit val timeout = Timeout(5 seconds)

  object StartMist {
    val threadMaster = {
      new Thread {
        override def run() = {
          s"./bin/mist start master --config ${TestConfig.cliConfig}" !
        }
      }
    }
  }

  class StartJob(route: String) {
    val threadMaster = {
      new Thread {
        override def run() = {
          s"bin/mist start job --config ${TestConfig.cliConfig} --route ${route}".!
        }
      }.start()
    }
  }

  override def afterAll(): Unit = {

    "./bin/mist stop".!

    TestKit.shutdownActorSystem(system)

    StartMist.threadMaster.join()
  }

  override def beforeAll(): Unit = {
    StartMist.threadMaster.start()
  }

  "CLI Workers" must {
    "list no workers" in {
      eventually(timeout(20 seconds), interval(5 seconds)) {
        val future = cliActor ? new ListMessage(Constants.CLI.listWorkersMsg)
        val result = Await.result(future, timeout.duration).asInstanceOf[String]
        assert(Constants.CLI.noWorkersMsg == result)
      }
    }

    "start streaming and list workers" in {

      var response = Constants.CLI.noWorkersMsg
      var result = Constants.CLI.noWorkersMsg


      val routers = List("streaming-1", "streaming-2", "streaming-3")

      routers.foreach {
        router =>
          new StartJob(router)
          eventually(timeout(20 seconds), interval(5 seconds)) {
            assert({
              response = result
              val future = cliActor ? new ListMessage(Constants.CLI.listWorkersMsg)
              result = Await.result(future, timeout.duration).asInstanceOf[String]
              response != result && response != Constants.CLI.noWorkersMsg
            })
          }
      }

    }

    "list two workers after kill first" in {
      val futureThreeWorkers = cliActor ? new ListMessage(Constants.CLI.listWorkersMsg)
      val resultThreeWorkers = Await.result(futureThreeWorkers, timeout.duration).asInstanceOf[String]
      assert(Constants.CLI.noWorkersMsg != resultThreeWorkers)

      cliActor ! new StringMessage(s"${Constants.CLI.stopWorkerMsg} streaming")

      eventually(timeout(20 seconds), interval(5 seconds)) {
        val futureTwoWorkers = cliActor ? new ListMessage(Constants.CLI.listWorkersMsg)
        val resultTwoWorkers = Await.result(futureTwoWorkers, timeout.duration).asInstanceOf[String]
        assert(resultThreeWorkers != resultTwoWorkers)
        assert(Constants.CLI.noWorkersMsg != resultTwoWorkers)
      }
    }

    "list two jobs" in {

    }

    "list two workers and one job after kill job" in {

    }

    "list no workers after kill all" in {
      val futureTwoWorkers = cliActor ? new ListMessage(Constants.CLI.listWorkersMsg)
      val resultTwoWorkers = Await.result(futureTwoWorkers, timeout.duration).asInstanceOf[String]
      assert(Constants.CLI.noWorkersMsg  != resultTwoWorkers)

      cliActor ! StopAllContexts
      eventually(timeout(20 seconds), interval(5 seconds)) {
        val futureNoWorkers = cliActor ? new ListMessage(Constants.CLI.listWorkersMsg)
        val resultNoWorkers = Await.result(futureNoWorkers, timeout.duration).asInstanceOf[String]
        assert(Constants.CLI.noWorkersMsg == resultNoWorkers)
      }
    }
  }
}