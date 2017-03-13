package io.hydrosphere.mist.contexts

import io.hydrosphere.mist.mistApi.ContextWrapper
import org.apache.spark.SparkContext

private[mist] case class NamedContextWrapper(context: SparkContext, namespace: String) extends ContextWrapper
