package io.hydrosphere.mist.jobs.runners.jar

import java.net.{URL, URLClassLoader}

import io.hydrosphere.mist.Constants
import io.hydrosphere.mist.contexts.ContextWrapper
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.jobs.{FullJobConfiguration, JobFile}
import io.hydrosphere.mist.lib.MistJob

private[mist] class JarRunner(jobConfiguration: FullJobConfiguration, jobFile: JobFile, contextWrapper: ContextWrapper) extends Runner {

  override val configuration: FullJobConfiguration = jobConfiguration

  val cls = {
    val classLoader = new URLClassLoader(Array[URL](jobFile.file.toURI.toURL), getClass.getClassLoader)
    classLoader.loadClass(configuration.className)
  }

  // Scala `object` reference of user job
  val objectRef = cls.getField("MODULE$").get(None)

  // We must add user`s jar into spark context
  contextWrapper.addJar(jobFile.file.getPath)

  _status = Runner.Status.Initialized

  override def run(): Either[Map[String, Any], String] = {
    _status = Runner.Status.Running
    try {
      val result = objectRef match {
        case objectRef: MistJob =>
          objectRef.setup(contextWrapper)
          Left(objectRef.doStuff(configuration.parameters))
        case _ => Right(Constants.Errors.notJobSubclass)
      }

      _status = Runner.Status.Stopped

      result
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
        _status = Runner.Status.Aborted
        Right(e.toString)
    }
  }

}
