package io.hydrosphere.mist.jobs

sealed trait JobConfiguration

abstract class FullJobConfiguration extends JobConfiguration {
  def path: String
  def className: String
  def namespace: String
  def parameters: Map[String, Any] = Map()
  def externalId: Option[String] = None
}

abstract class RestificatedJobConfiguration extends JobConfiguration {
  def route: String
  def parameters: Map[String, Any] = Map()
  def externalId: Option[String] = None
}

/** Configuration for jobs
  *
  * @param path         user file with implemented spark jobs (.py or .jar)
  * @param className    class in jar we must to use to run job
  * @param namespace    context namespace
  * @param parameters   parameters for user job
  * @param externalId   optional external id used to differ async responses from mist
  */
private[mist] case class MistJobConfiguration(path: String,
                                              className: String,
                                              namespace: String,
                                              override val parameters: Map[String, Any] = Map(),
                                              override val externalId: Option[String] = None) extends FullJobConfiguration

private[mist] case class RestificatedMistJobConfiguration(route: String,
                                                          override val parameters: Map[String, Any] = Map(),
                                                          override val externalId: Option[String] = None) extends RestificatedJobConfiguration

private[mist] case class TrainingJobConfiguration(path: String,
                                                  className: String,
                                                  namespace: String,
                                                  override val parameters: Map[String, Any] = Map(),
                                                  override val externalId: Option[String] = None) extends FullJobConfiguration

private[mist] case class ServingJobConfiguration(path: String,
                                                  className: String,
                                                  namespace: String = null,
                                                  override val parameters: Map[String, Any] = Map(),
                                                  override val externalId: Option[String] = None) extends FullJobConfiguration


private[mist] case class RestificatedTrainingJobConfiguration(route: String,
                                                  override val parameters: Map[String, Any] = Map(),
                                                  override val externalId: Option[String] = None) extends RestificatedJobConfiguration

private[mist] case class RestificatedServingJobConfiguration(route: String,
                                                              override val parameters: Map[String, Any] = Map(),
                                                              override val externalId: Option[String] = None) extends RestificatedJobConfiguration
