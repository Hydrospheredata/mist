package com.provectus.lymph.jobs

/** Configuration for jobs
  *
  * @param jarPath      user file with implemented spark jobs
  * @param className    class in jar we must to use to run job
  * @param name         context namespace
  * @param parameters   parameters for user job
  * @param external_id  optional external id used to differ async answers from lymph
  */
private[lymph] case class JobConfiguration(jarPath: Option[String] = None, pyPath: Option[String] = None, className: Option[String] = None, name: String, parameters: Map[String, Any] = Map(), external_id: Option[String] = None)