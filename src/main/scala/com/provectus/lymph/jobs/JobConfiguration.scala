package com.provectus.lymph.jobs

private[lymph] case class JobConfiguration(jarPath: String, className: String, name: String, parameters: Map[String, Any] = Map(), external_id: Option[String] = None)
