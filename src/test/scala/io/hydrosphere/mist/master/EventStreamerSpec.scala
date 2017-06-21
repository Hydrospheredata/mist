package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigRenderOptions, ConfigValueType}
import io.hydrosphere.mist.Messages.StatusMessages.StartedEvent
import io.hydrosphere.mist.MistConfig
import org.scalatest.{FunSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class EventStreamerSpec extends TestKit(ActorSystem("streamer"))
  with FunSpecLike
  with Matchers {

  implicit val materializer = ActorMaterializer()

  it("should broadcast events") {
    val streamer = EventsStreamer(system)

    val f = streamer.eventsSource()
      .take(2)
      .runWith(Sink.seq)

    streamer.push(StartedEvent("1", 1))
    streamer.push(StartedEvent("2", 1))
    streamer.push(StartedEvent("3", 1))

    val events = Await.result(f, Duration.Inf)
    events should contain allOf (
      StartedEvent("1", 1),
      StartedEvent("2", 1)
    )
  }

  it("asda") {
    import scala.collection.JavaConverters._

    def find(s: String, cfg: Config, path: Seq[String]): Option[Seq[String]] = {
      val entrys = cfg.root().entrySet()
      entrys.asScala.find(e => e.getKey == s) match {
        case None =>
          val objects = entrys.asScala.filter(e => e.getValue.valueType() == ConfigValueType.OBJECT)
          objects.map(e => {
            val next = cfg.getConfig(e.getKey)
            find(s, next, path :+ e.getKey)
          }).collectFirst({case Some(x) => x})

        case Some(x) => Some(path)
      }
    }

    val system = ActorSystem("mist", MistConfig.Akka.Main.settings)
    val cfg = system.dispatchers.cachingConfig
    println(find("writers-blocking-dispatcher", cfg, Seq.empty))
    println(system.dispatchers.hasDispatcher("writers-blocking-dispatcher"))
//    println(system.dispatchers.cachingConfig.root().render(ConfigRenderOptions.defaults()))
  }
}
