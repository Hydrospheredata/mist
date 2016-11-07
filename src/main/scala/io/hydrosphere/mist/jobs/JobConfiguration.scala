package io.hydrosphere.mist.jobs

sealed trait JobConfiguration

/** Configuration for jobs
  *
  * @param path         user file with implemented spark jobs (.py or .jar)
  * @param className    class in jar we must to use to run job
  * @param namespace         context namespace
  * @param parameters   parameters for user job
  * @param externalId  optional external id used to differ async responses from mist
  */
private[mist] case class FullJobConfiguration(path: String,
                                              className: String,
                                              namespace: String,
                                              parameters: Map[String, Any] = Map(),
                                              externalId: Option[String] = None) extends JobConfiguration

private[mist] case class RestificatedJobConfiguration(route: String, parameters: Map[String, Any] = Map()) extends JobConfiguration