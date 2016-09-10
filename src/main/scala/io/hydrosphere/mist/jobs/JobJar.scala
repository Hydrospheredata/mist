package io.hydrosphere.mist.jobs

import java.io.File
import java.net.{URL, URLClassLoader}
import io.hydrosphere.mist.contexts.ContextWrapper
import java.io.FileInputStream
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

    val classLoader = configuration.jarPath.get.split(':').headOption.getOrElse("") match {
      case "hdfs" => {
        val conf = new Configuration()
        val fileSystem = FileSystem.get(conf)
        val path = new Path(configuration.jarPath.get)
        val localpath = new Path(path.getName())
        fileSystem.copyToLocalFile(false, path, localpath, true)
        val jarFile = new File(path.getName())
        //val istream = fileSystem.open(path)
        //val jarFile = istream.getFileDescriptor()
       /*
        val uriPath = path.toUri()
        val urlPath = uriPath.toURL()
        val jarFile = urlPath.getFile
        println(jarFile)
        val tmpFile = new File("job.tmp")
        val fos = new FileOutputStream(tmpFile)
        fos.write(istream.getBytes())
        */
        new URLClassLoader(Array[URL](jarFile.toURI.toURL), getClass.getClassLoader)    
      }
      case _ => {
        val jarFile = new File(configuration.jarPath.get)
        new URLClassLoader(Array[URL](jarFile.toURI.toURL), getClass.getClassLoader)    
      }
    }

    
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