package io.hydrosphere.mist.aws

import com.typesafe.config.{Config, ConfigFactory}
import io.hydrosphere.mist.utils.ConfigUtils._

case class ProvisionData(
  sshKeyPair: String,
  sshKeyPath: String,
  accessKey: String,
  secretKey: String,
  subnetId: String,
  region: String,
  additionalGroup: String,
  emrRole: String,
  emrEc2Role: String
)

object ConfigPatcher {

  def path(mistConfig: Config, provisionData: ProvisionData): Config = {
    import com.typesafe.config.ConfigValueFactory._
    import scala.collection.JavaConverters._

    import provisionData._

    val configKeys = Map(
      "name" -> "default_emr",
      "type" -> "aws_emr",
      "sshKeyPair" -> sshKeyPair,
      "sshKeyPath" -> sshKeyPath,
      "accessKey"-> accessKey,
      "secretKey" -> secretKey,
      "subnetId" -> subnetId,
      "region" -> region,
      "additionalGroup" -> additionalGroup,
      "emrRole" -> emrRole,
      "emrEc2Role" -> emrEc2Role
    ).map({case (k, v) => k -> fromAnyRef(v)})

    val provisioner = fromMap(configKeys.asJava).toConfig
    val entries = fromIterable(Seq(provisioner).asJava)

    mistConfig.withValue("provision", entries)
  }

}

