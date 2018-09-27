package io.hydrosphere.mist.master.execution.aws

import java.nio.file.Paths

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

    val agentRemoteJar = s"$targetDir/mist-agent.jar"
    Seq(
      SSHCmd.Exec(Seq("mkdir", targetDir)),
      SSHCmd.CopyFile(agentJar, s"$targetDir/mist-agent.jar"),
      SSHCmd.CopyFile(workerJar, s"$targetDir/mist-worker.jar"),
      SSHCmd.Exec(Seq(
        "java", "-cp", agentRemoteJar, "io.hydrosphere.mist.agent.ClusterAgent",
        masterAddress, agentId, accessKey, secretKey, region, clusterId,
        s"1>$targetDir/out.log", s"2>$targetDir/out.log", "&"
      ))
    )
  }

}

object Test {

  def main(args: Array[String]): Unit = {
    val jarsDir = Paths.get("/home/dos65/projects/mist/target/mist-1.0.0-RC17")
    val realDir = jarsDir.toAbsolutePath.toRealPath()

    val host = "ec2-18-196-156-203.eu-central-1.compute.amazonaws.com"
    val sshUser = "hadoop"
    val agentId= "j-2NVSS1JAKYOEU"

    val akkaAddress = "blabla.com:2005"

    val accessKey = "AKIA5XGMCUEIJ3NOW6Y3"
    val secretKey = "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"
    val region = "eu-central-1"


    val transfer = TransferParams(
      realDir.resolve("mist-agent.jar").toString,
      realDir.resolve("mist-worker.jar").toString,
      s"/home/$sshUser/mist-agent"
    )
    val agentRunParams = AgentRunParams(
      agentId, akkaAddress, accessKey, secretKey, region, agentId
    )
    val cmds = AgentInstall.sshCommands(transfer, agentRunParams)
    val out = new SSHClient(host, sshUser, "/home/dos65/.ssh/mist_key").install(cmds)

    out match {
      case scala.util.Failure(exception) => exception.printStackTrace()
      case scala.util.Success(value) => println("OOK")
    }
  }
}
