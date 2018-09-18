package io.hydrosphere.mist.aws

import java.nio.file.Paths

object Main {

  def main(args: Array[String]): Unit = {
    val instanceId = args(0)
    val accessKey = args(1)
    val accessSecret = args(2)
    val region = args(3)
    val configPath = args(4)

    val sshKeyPair = args(5)
    val sshKeyPath = args(6)

    val setup = AwsSetup.create(accessKey, accessSecret, region)
    val out = setup.setup(instanceId).unsafeRunSync()

    val provisionData = ProvisionData(
      sshKeyPair = sshKeyPair,
      sshKeyPath = sshKeyPath,
      accessKey = accessKey,
      secretKey = accessSecret,
      subnetId = out.subnetId,
      region = region,
      additionalGroup = out.securityGroupId,
      emrRole = out.emrRole,
      emrEc2Role = out.ec2EmrRole
    )

    ConfigPatcher.patchFile(Paths.get(configPath), provisionData)
  }

}
