package io.hydrosphere.mist.contexts

import org.apache.spark.SparkContext

private[mist] case class NamedContextWrapper(context: SparkContext, name: String) extends ContextWrapper