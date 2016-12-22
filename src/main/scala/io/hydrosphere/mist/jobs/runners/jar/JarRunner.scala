package io.hydrosphere.mist.jobs.runners.jar

import java.net.{URL, URLClassLoader}

import io.hydrosphere.mist.Constants
import io.hydrosphere.mist.contexts.ContextWrapper
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.lib.{MLMistJob, MistJob}

private[mist] class JarRunner(override val configuration: FullJobConfiguration, jobFile: JobFile, contextWrapper: ContextWrapper) extends Runner {

  val cls: Class[_] = {
    val classLoader = new URLClassLoader(Array[URL](jobFile.file.toURI.toURL), getClass.getClassLoader)
    classLoader.loadClass(configuration.className)
  }

  // Scala `object` reference of user job
  val objectRef: AnyRef = cls.getField("MODULE$").get(None)

  // TODO: remove nullable contextWrapper 
  if (contextWrapper != null) {
    // We must add user jar into spark context
    contextWrapper.addJar(jobFile.file.getPath)
  }

  _status = Runner.Status.Initialized

  override def run(): Either[Map[String, Any], String] = {
    _status = Runner.Status.Running
    new Thread().run()
    try {
      val result = configuration match {
        case _: MistJobConfiguration =>
          objectRef.asInstanceOf[MistJob].setup(contextWrapper)
          Left(objectRef.asInstanceOf[MistJob].doStuff(configuration.parameters))
        case _: TrainingJobConfiguration =>
          objectRef.asInstanceOf[MLMistJob].setup(contextWrapper)
          Left(objectRef.asInstanceOf[MLMistJob].train(configuration.parameters))
        case _: ServingJobConfiguration =>
          Left(objectRef.asInstanceOf[MLMistJob].serve(configuration.parameters))
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
