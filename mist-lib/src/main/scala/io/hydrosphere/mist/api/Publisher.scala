package io.hydrosphere.mist.api

import java.io.ByteArrayOutputStream
import java.net.Socket
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate}

import com.twitter.chill.ScalaKryoInstantiator
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{AppenderSkeleton, Level, Logger}

trait MistLogging extends ContextSupport {

  private var loggingConf: Option[CentralLoggingConf] = None
  private var tcpAppender: TcpAppender = _
  private var jobId: String = _

  override private[mist] def setup(conf: SetupConfiguration): Unit = {
    super.setup(conf)
    this.loggingConf = conf.loggingConf
    this.jobId = conf.info.id
  }

  override private[mist] def stop(): Unit = {
    if (tcpAppender != null)
      tcpAppender.close()
  }

  @transient
  lazy val logger = {
    val logger = Logger.getLogger(getClass)

    loggingConf.foreach(cfg => {
      tcpAppender = new TcpAppender(cfg.host, cfg.port, jobId)
      logger.addAppender(tcpAppender)
    })

    logger
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
    s"$log4jLevel $date [$from] $message $error"
  }

  private def formatDate: String = {
    val inst = Instant.ofEpochMilli(timeStamp)
    DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(inst)
  }
}


private[mist] class TcpAppender(
  host: String,
  port: Int,
  from: String) extends AppenderSkeleton {

  import com.esotericsoftware.kryo.io.Output

  val instantiator = new ScalaKryoInstantiator
  instantiator.setRegistrationRequired(false)
  val kryo = instantiator.newKryo()

  val socket = new Socket(host, port)
  val outStream = socket.getOutputStream

  override def append(e: LoggingEvent): Unit = {
    val event = LogEvent(
       from,
       e.getRenderedMessage,
       e.timeStamp,
       e.getLevel.toInt,
       e.getThrowableStrRep
    )

    val bytesOut = new ByteArrayOutputStream(512)
    val output = new Output(bytesOut)
    kryo.writeObject(output, event)
    val bytes = output.getBuffer

    outStream.write(bytes)
    outStream.flush()
  }

  override def requiresLayout(): Boolean = false

  override def close(): Unit = {
    socket.close()
  }
}

