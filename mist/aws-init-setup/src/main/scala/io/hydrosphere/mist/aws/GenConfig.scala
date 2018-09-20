package io.hydrosphere.mist.aws

import java.nio.file.{Files, Path}

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}

case class LaunchData(
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

  def patch(mistConfig: Config, data: LaunchData): Config = {
    import com.typesafe.config.ConfigValueFactory._
    import scala.collection.JavaConverters._

    import data._

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

    val provisioner = fromMap(configKeys.asJava)
    val entries = fromIterable(Seq(provisioner).asJava)

    mistConfig.withValue("mist.launchers-settings", entries)
  }

  def patchFile(filePath: Path, data: LaunchData): Unit = {
    val orig = ConfigFactory.parseFile(filePath.toFile)
    val patched = patch(orig, data)

    val renderOpts = ConfigRenderOptions.defaults()
      .setComments(false)
      .setOriginComments(false)
      .setJson(false)
      .setFormatted(true)

    val rawConfig = patched.root().render(renderOpts)
    Files.write(filePath, rawConfig.getBytes)
  }

}

