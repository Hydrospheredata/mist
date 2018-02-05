package io.hydrosphere.mist.master.interfaces.http

import java.lang.management._
import java.time.LocalDateTime

import io.hydrosphere.mist.core.jvmjob.FunctionInfoData
import io.hydrosphere.mist.master.models.ContextConfig
import mist.api.args.UserInputArgument

import scala.concurrent.duration.Duration

case class HttpJobInfo(
  name: String,
  execute: Option[Map[String, HttpJobArg]] = None,
  serve: Option[Map[String, HttpJobArg]] = None,

  isHiveJob: Boolean = false,
  isSqlJob: Boolean = false,
  isStreamingJob: Boolean = false,
  isMLJob: Boolean = false,
  isPython: Boolean = false
)


object HttpJobInfo {

  def forPython(name: String) = HttpJobInfo(name = name, isPython = true)

  def convert(info: FunctionInfoData): HttpJobInfo = {
    val argsMap = info.execute
      .collect { case u: UserInputArgument => u }
      .map { a => a.name -> HttpJobArg.convert(a.t) }
      .toMap

    val jobInfo = HttpJobInfo(
      name = info.name,
      isPython = info.lang == FunctionInfoData.PythonLang
    )

    if (info.isServe)
      jobInfo.copy(serve = Some(argsMap))
    else jobInfo.copy(execute = Some(argsMap))

  }
}

case class HttpJobArg(
  `type`: String,
  args: Option[Seq[HttpJobArg]],
  fields: Option[Map[String, HttpJobArg]] = None
)

object HttpJobArg {

  import cats.syntax.option._
  import mist.api.args._

  def plain(`type`: String): HttpJobArg =
    HttpJobArg(`type`, None, None)

  def withTypeArgs(`type`: String, args: Seq[HttpJobArg]): HttpJobArg =
    HttpJobArg(`type`, args.some)

  def complex(`type`: String, fields: Map[String, HttpJobArg]): HttpJobArg =
    HttpJobArg(`type`, None, fields.some)

  def convert(argType: ArgType): HttpJobArg = {
    val t = argType.getClass.getSimpleName.replace("$", "")
    argType match {
      case x@(MBoolean | MInt | MDouble | MString | MAny) => plain(t)
      case x: MMap => withTypeArgs(t, Seq(x.k, x.v).map(convert))
      case x: MList => withTypeArgs(t, Seq(convert(x.v)))
      case x: MOption => withTypeArgs(t, Seq(convert(x.v)))
      case x: MObj => complex(t, x.fields.map({ case (k, v) => k -> convert(v) }).toMap)
    }
  }
}


case class HttpEndpointInfoV2(
  name: String,
  lang: String,
  execute: Map[String, HttpJobArg] = Map.empty,

  tags: Seq[String] = Seq.empty,

  path: String,
  className: String,
  defaultContext: String

)

object HttpEndpointInfoV2 {

  def convert(info: FunctionInfoData): HttpEndpointInfoV2 = {
    HttpEndpointInfoV2(
      name = info.name,
      path = info.path,
      className = info.className,
      tags = info.tags,
      defaultContext = info.defaultContext,
      execute = info.execute
        .map(a => a.name -> HttpJobArg.convert(a.t))
        .toMap,
      lang = info.lang
    )
  }
}

case class EndpointCreateRequest(
  name: String,
  path: String,
  className: String,
  nameSpace: String
)

case class ContextCreateRequest(
  name: String,
  sparkConf: Option[Map[String, String]],
  downtime: Option[Duration],
  maxJobs: Option[Int],
  precreated: Option[Boolean],
  workerMode: Option[String],
  runOptions: Option[String] = None,
  streamingDuration: Option[Duration]
) {

  workerMode match {
    case Some(m) =>
      require(ContextCreateRequest.AvailableRunMode.contains(m),
        s"Worker mode should be in ${ContextCreateRequest.AvailableRunMode}")
    case _ =>
  }

  def toContextWithFallback(other: ContextConfig): ContextConfig =
    ContextConfig(
      name,
      sparkConf.getOrElse(other.sparkConf),
      downtime.getOrElse(other.downtime),
      maxJobs.getOrElse(other.maxJobs),
      precreated.getOrElse(other.precreated),
      runOptions.getOrElse(other.runOptions),
      workerMode.getOrElse(other.workerMode),
      streamingDuration.getOrElse(other.streamingDuration)
    )
}

object ContextCreateRequest {
  val AvailableRunMode = Set("shared", "exclusive")
}


case class MistStatus(
  mistVersion: String,
  sparkVersion: String,
  started: LocalDateTime,
  gc: Map[String, GCMetrics],
  memory: HeapMetrics,
  threads: ThreadMetrics,
  javaVersion: JavaVersionInfo
)

object MistStatus {

  import io.hydrosphere.mist.BuildInfo

  import scala.collection.JavaConverters._

  val Started = LocalDateTime.now()
  val SparkVersion = {
    val is1x = BuildInfo.sparkVersion.startsWith("1.")
    if (is1x) "1.x.x" else "2.x.x"
  }

  def create: MistStatus = {
    val beans = ManagementFactory.getGarbageCollectorMXBeans.asScala
    val memoryMXBean = ManagementFactory.getMemoryMXBean
    val threadMXBean = ManagementFactory.getThreadMXBean

    val gCMetrics = beans.map(gc => s"${gc.getName}" -> GCMetrics.create(gc)).toMap
    MistStatus(
      BuildInfo.version,
      SparkVersion,
      Started,
      gCMetrics,
      HeapMetrics.create(memoryMXBean),
      ThreadMetrics.create(threadMXBean),
      JavaVersionInfo.create
    )
  }
}

case class GCMetrics(collectionCount: Long, collectionTimeInSec: Long)

object GCMetrics {
  def create(gc: GarbageCollectorMXBean): GCMetrics = GCMetrics(
    gc.getCollectionCount, gc.getCollectionTime / 1000
  )
}


case class Heap(used: Long, commited: Long, max: Long, init: Long)

object Heap {
  val BytesPerMegabyte = 1024 * 1024

  def create(memory: MemoryUsage): Heap = {
    Heap(
      memory.getUsed / BytesPerMegabyte,
      memory.getCommitted / BytesPerMegabyte,
      memory.getMax / BytesPerMegabyte,
      memory.getInit / BytesPerMegabyte
    )
  }
}

case class HeapMetrics(heap: Heap, nonHeap: Heap)

object HeapMetrics {

  def create(memoryMXBean: MemoryMXBean): HeapMetrics = {
    HeapMetrics(
      Heap.create(memoryMXBean.getHeapMemoryUsage),
      Heap.create(memoryMXBean.getNonHeapMemoryUsage)
    )
  }
}

case class ThreadMetrics(
  count: Long,
  daemon: Long,
  peak: Long,
  startedTotal: Long,
  deadlocked: Option[Long],
  deadlockedMonitor: Option[Long]
)

object ThreadMetrics {
  def create(threadMXBean: ThreadMXBean): ThreadMetrics = {
    ThreadMetrics(
      threadMXBean.getThreadCount.toLong,
      threadMXBean.getDaemonThreadCount.toLong,
      threadMXBean.getPeakThreadCount.toLong,
      threadMXBean.getTotalStartedThreadCount,
      Option(threadMXBean.findDeadlockedThreads()).map(_.length.toLong),
      Option(threadMXBean.findMonitorDeadlockedThreads()).map(_.length.toLong)
    )
  }
}

case class JavaVersionInfo(runtimeVersion: String, vmVendor: String)

object JavaVersionInfo {
  def create: JavaVersionInfo = {
    JavaVersionInfo(
      System.getProperty("java.runtime.version", "unknown"),
      System.getProperty("java.vm.vendor", "unknown")
    )
  }
}