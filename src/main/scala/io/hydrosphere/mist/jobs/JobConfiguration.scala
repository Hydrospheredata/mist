package io.hydrosphere.mist.jobs

/** Configuration for jobs
  *
  * @param path         user file with implemented spark jobs (.py or .jar)
  * @param className    class in jar we must to use to run job
  * @param name         context namespace
  * @param parameters   parameters for user job
  * @param external_id  optional external id used to differ async responses from mist
  */
private[mist] case class JobConfiguration(path: String,
                                          className: String,
                                          name: String,
                                          parameters: Map[String, Any] = Map(),
                                          external_id: Option[String] = None)
