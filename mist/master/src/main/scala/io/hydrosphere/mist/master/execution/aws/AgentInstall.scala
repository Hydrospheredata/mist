package io.hydrosphere.mist.master.execution.aws

case class TransferParams(
  agentJar: String,
  workerJar: String,
  targetDir: String
)

case class AgentRunParams(
  agentId: String,
  masterAddress: String,
  accessKey: String,
  secretKey: String,
  region: String,
  clusterId: String
)

object AgentInstall {

  def sshCommands(
    transferParams: TransferParams,
    agentRunParams: AgentRunParams
  ): Seq[SSHCmd] = {

    import transferParams._
    import agentRunParams._

    Seq(
      SSHCmd.Exec(Seq("mkdir", targetDir)),
      SSHCmd.CopyFile(agentJar, s"$targetDir/mist-agent.jar"),
      SSHCmd.CopyFile(workerJar, s"$targetDir/mist-worker.jar"),
      SSHCmd.Exec(Seq(
        "java", "-cp", "~/mist-agent/mist-master.jar", "io.hydrosphere.mist.master.execution.aws.ClusterAgent",
        masterAddress, agentId, accessKey, secretKey, region, clusterId,
        s"1>$targetDir/out.log", s"2>$targetDir/out.log", "&"
      ))
    )
  }

}
