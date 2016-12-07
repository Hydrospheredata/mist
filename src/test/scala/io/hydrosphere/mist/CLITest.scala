package  io.hydrosphere.mist

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import io.hydrosphere.mist.Messages.ListMessage
import io.hydrosphere.mist.worker.CLINode
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class CLITest extends WordSpecLike with BeforeAndAfterAll {

  implicit val system = ActorSystem("mist", MistConfig.Akka.CLI.settings)
  val cliActor = system.actorOf(Props[CLINode], name = Constants.CLI.cliActorName )

  /*
  override def afterAll() = {
  }
*/


 /* override def beforeAll() = {

  }*/


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