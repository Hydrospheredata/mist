package io.hydrosphere.mist.api

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source, Tcp}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.util.ByteString
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Level
import org.apache.spark.TaskContext

trait MistLogging extends ContextSupport {

  private var loggingConf: Option[CentralLoggingConf] = None
  private var jobId: String = _

  override private[mist] def setup(conf: SetupConfiguration): Unit = {
    super.setup(conf)
    this.loggingConf = conf.loggingConf
    this.jobId = conf.info.id
  }

  def getLogger: MLogger = loggingConf match {
    case Some(conf) => MLogger(conf.host, conf.port, jobId)
    case None => throw new IllegalStateException("Can not instantiate logger here")
  }

  override private[mist] def stop(): Unit = {
    super.stop()
  }

}

private[mist] case class LogEvent(
  from: String,
  message: String,
  timeStamp: Long,
  level: Int,
  throwable: Array[String]) {

  def mkString: String = {
    val date = formatDate
    val error = if (throwable.length > 0) throwable.mkString("\nError:", "\n\t", "") else ""
    s"$formatLevel $date [$from] $message $error"
  }

  private def formatDate: String = {
//    val inst = Instant.ofEpochMilli(timeStamp)
    //val date = LocalDateTime.ofInstant(inst.atZone(ZoneId.))
//    val date = timeStamp.toString
//    DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(inst)
    timeStamp.toString
  }

  private def formatLevel: String = Level.toLevel(level).toString
}

case class MLogger(
  host: String,
  port: Int,
  sourceId: String
) {

  @transient
  lazy val writer = {
    val writer = SocketWriter.newWriter(host, port)
    val taskCtx = TaskContext.get()
    if (taskCtx != null)
      taskCtx.addTaskCompletionListener(_ => SocketWriter.deregister())
    writer
  }

  def info(msg: String): Unit = synchronized {
    val event = LogEvent(
      sourceId,
      msg,
      System.currentTimeMillis(),
      2000,
      Array.empty
    )
    writer.write(event)
  }
}

class Writer(host: String, port: Int) {

  implicit val sys = ActorSystem("log-writer", ConfigFactory.empty)
  implicit val mat = ActorMaterializer()

  val kryoPool = {
    val inst = new ScalaKryoInstantiator
    inst.setRegistrationRequired(false)
    KryoPool.withByteArrayOutputStream(10, inst)
  }

  val inputActor = {
    def encode(a: Any): ByteString = {
      val body = ByteString(kryoPool.toBytesWithoutClass(a))
      val length = body.length
      val head = new Array[Byte](4)
      head(0) = (length >> 24).toByte
      head(1) = (length >> 16).toByte
      head(2) = (length >> 8).toByte
      head(3) = length.toByte

      ByteString(head) ++ body
    }

    Source.actorRef[LogEvent](1000, OverflowStrategy.dropHead)
      .map(encode)
      .via(Tcp().outgoingConnection(host, port))
      .toMat(Sink.ignore)(Keep.left).run()
  }

  def write(e: LogEvent): Unit = {
    inputActor ! e
  }

  def close(): Unit = {
    println("TRY CLOSE")
  }

}

object SocketWriter {

  val links = new AtomicInteger(0)
  private var writer: Writer = null

  def newWriter(host: String, port: Int): Writer = {
    if (links.get == 0) {
      synchronized {
        writer = new Writer(host, port)
      }
    }
    links.incrementAndGet()
    writer
  }

  def deregister(): Unit = {
    println(s"Deregister ${links.get}")
    if (links.decrementAndGet == 0 ) {
      synchronized {
        //writer.close()
        println("CLOSE ACTION")
        writer = null
      }
    }
  }
}
