package io.hydrosphere.mist.jobs.runners.jar

import cats.implicits._
import io.hydrosphere.mist.contexts.NamedContext
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.utils.TypeAlias.JobResponseOrError

class JarRunner(
  override val job: JobDetails,
  jobFile: JobFile,
  context: NamedContext) extends Runner {

  context.addJar(jobFile.file.getPath)

  //TODO: swap either types - result should be in right position
  override def run(): JobResponseOrError = {
    val clazz = job.configuration.className
    val action = job.configuration.action
    val params = job.configuration.parameters

    val load = JobsLoader.fromJar(jobFile.file).loadJobInstance(clazz, action)
    val result = Either.fromTry(load).flatMap(instance => {
      instance.run(context.setupConfiguration, params)
    })

    result.swap.map(_.getMessage)
  }

  //TODO: stop is kill job??
  override def stop(): Unit = {

  }
}
