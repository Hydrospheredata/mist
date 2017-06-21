package io.hydrosphere.mist.master.logging

import java.nio.ByteOrder

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import akka.stream.io.Framing
import akka.stream.scaladsl.{Tcp, _}
import akka.util.ByteString
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import io.hydrosphere.mist.api.logging.MistLogging.LogEvent
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.Future

object TcpServer extends Logger {


//  def start(host: String, port: Int, logStore: LogsStore, statusS: ActorRef)
//    (implicit sys: ActorSystem, m: ActorMaterializer): Future[Tcp.ServerBinding] = {
//
//    val source = Tcp().bind(host, port)
//
//    source.toMat(Sink.foreach(conn => {
//      val x = Flow[ByteString]
//        .via(Framing.lengthField(4, 0, 1024 * 1024 * 8, ByteOrder.BIG_ENDIAN))
//        .map(bs => {
//          val bytes = bs.drop(4).toArray
//          kryoPool.fromBytes(bytes, classOf[LogEvent])
//        })
//        .map(e => {logStore.store(e); e})
//        .map(e => statusS ! e)
//        .map(_ => {
//          ByteString.empty
//        })
//        .filter(_ => false)
//      conn.handleWith(x)
//    }))(Keep.left).run()
//  }

//  def connectionFlow[A, B](
//    eventHandler: Flow[A, B, Any],
//    decodePoolSize: Int = 10): Flow[ByteString, ByteString, Unit] = {
//
//    val kryoPool = {
//      val inst = new ScalaKryoInstantiator
//      inst.setRegistrationRequired(false)
//      KryoPool.withByteArrayOutputStream(decodePoolSize, inst)
//    }
//
//    Flow[ByteString]
//      .via(Framing.lengthField(4, 0, 1024 * 1024 * 8, ByteOrder.BIG_ENDIAN))
//      .map(bs => {
//        val bytes = bs.drop(4).toArray
//        kryoPool.fromBytes(bytes, classOf[A])
//      })
//      .via(eventHandler)
//      .map(_ => ByteString.empty)
//      .filter(_ => false)
//  }

}

