package io.hydrosphere.mist.jobs

import java.io.File
import java.net.{URL, URLClassLoader}
import io.hydrosphere.mist.contexts.ContextWrapper


/** Class-container for user jobs
  *
  * @param jobConfiguration [[io.hydrosphere.mist.jobs.JobConfiguration]] instance
  * @param contextWrapper   contexts for concrete job running
  */
private[mist] class JobJar(jobConfiguration: JobConfiguration, contextWrapper: ContextWrapper, JobRunnerName: String) extends Job with JobJarRun{

  override val jobRunnerName = JobRunnerName

  override val configuration = jobConfiguration

  // Class with job in user jar
  override val cls = try{
    val jarFile = new File(configuration.jarPath.get)
    val classLoader = new URLClassLoader(Array[URL](jarFile.toURI.toURL), getClass.getClassLoader)
    classLoader.loadClass(configuration.className.get)
  } catch {
    case e: Throwable =>
      throw new Exception(e)
  }

  // Scala `object` reference of user job
  override val objectRef = cls.getField("MODULE$").get(None)

  // We must add user jar into spark context
  contextWrapper.addJar(configuration.jarPath.get)

  _status = JobStatus.Initialized

}