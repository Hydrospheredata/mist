package io.hydrosphere.mist.jobs

import java.io.File
import java.net.{URI, URL, URLClassLoader}

import io.hydrosphere.mist.contexts.ContextWrapper

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._


/** Class-container for user jobs
  *
  * @param jobConfiguration [[io.hydrosphere.mist.jobs.JobConfiguration]] instance
  * @param _contextWrapper   contexts for concrete job running
  */
private[mist] class JobJar(jobConfiguration: JobConfiguration, _contextWrapper: ContextWrapper, JobRunnerName: String) extends JobJarRun{

  override val contextWrapper = _contextWrapper

  override val jobRunnerName = JobRunnerName

  override val configuration = jobConfiguration


  // Class with job in user jar
  override val cls = try{

    val jarFile = configuration.jarPath.get.split(':').headOption.getOrElse("") match {
      case "hdfs" =>
        val uriParts = configuration.jarPath.get.replaceFirst("hdfs://", "").split("/", 2)
        val fileSystem = FileSystem.get(new URI(s"hdfs://${uriParts(0)}"), new Configuration())
        val remotePath = new Path(configuration.jarPath.get)
        val localPath = new Path(s"/tmp/${remotePath.getName}")
        fileSystem.copyToLocalFile(false, remotePath, localPath, true)

        new File(localPath.toString)

      case _ => new File(configuration.jarPath.get)
    }

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