package  io.hydrosphere.mist

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.TestKit
import akka.util.Timeout
import io.hydrosphere.mist.Messages.ListMessage
import io.hydrosphere.mist.worker.CLINode
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.sys.process._

class CLITest extends WordSpecLike with BeforeAndAfterAll {

  implicit val system = ActorSystem("mist", MistConfig.Akka.CLI.settings)
  val cliActor = system.actorOf(Props[CLINode], name = Constants.CLI.cliActorName )

  object StartMist {
    val threadMaster = {
      new Thread {
        override def run() = {
          s"./bin/mist start master --config src/test/resources/cli.conf" !
        }
      }
    }
  }

  override def afterAll(): Unit = {

    "./bin/mist stop".!

    TestKit.shutdownActorSystem(system)

    StartMist.threadMaster.join()
  }

  override def beforeAll(): Unit = {
    StartMist.threadMaster.start()
    Thread.sleep(10000)
  }

  "CLI Workers" must {
    "list no workers" in {
      implicit val timeout = Timeout(5 seconds)
      val future = cliActor ? new ListMessage(Constants.CLI.listWorkersMsg)
      val result = Await.result(future, timeout.duration).asInstanceOf[String]
      assert(Constants.CLI.noWorkersMsg  == result)
    }

    "list three workers" in {

    }

    "list two workers after kill first" in {

    }

    "list two jobs" in {

    }

    "list two workers and one job after kill job" in {

    }

    "list no workers again after kill all" in {

    }
  }
}