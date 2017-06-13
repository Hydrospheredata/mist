package io.hydrosphere.mist.master.logging

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Tcp, _}
import akka.util.ByteString
import com.esotericsoftware.kryo.io.Input
import com.twitter.chill.ScalaKryoInstantiator
import io.hydrosphere.mist.api.LogEvent
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.Future

object TcpServer extends Logger {

  def start(host: String, port: Int)
    (implicit sys: ActorSystem, m: ActorMaterializer): Future[Tcp.ServerBinding] = {

    val source = Tcp().bind(host, port)

    source.toMat(Sink.foreach(conn => {
      val kryoDeserializer = new KryoDeserializer
      val x = Flow[ByteString]
        .map(bs => kryoDeserializer.deserialize(bs.toArray))
        .map(e => {
          ByteString.empty
        })
      conn.handleWith(x)
    }))(Keep.left).run()
  }

  class KryoDeserializer {

    val instantiator = new ScalaKryoInstantiator
    instantiator.setRegistrationRequired(false)
    val kryo = instantiator.newKryo()

    def deserialize(bytes: Array[Byte]): Option[LogEvent] = {
      try {
        val input = new Input(bytes)
        val event = kryo.readObject(input, classOf[LogEvent])
        Some(event)
      } catch {
        case e: Throwable =>
          val preview = bytes.slice(0, 10)
          logger.warn(s"Can not deserialize incoming message $preview", e)
          None
      }
    }
  }
}

