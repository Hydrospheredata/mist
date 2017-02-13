node('aws-slave') {
  wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'XTerm']) {

    try {
      stage('clone project') {
        checkout scm
      }

      def tag = sh(returnStdout: true, script: "git tag -l --contains HEAD").trim()

      stage('build and test') {
        parallel ( failFast: false,
          Spark_1_5_2: { test_mist("1.5.2") },
        )
      }
    }
    catch (err) {
      currentBuild.result = "FAILURE"
      echo "${err}"
      gitEmail = sh(returnStdout: true, script: "git --no-pager show -s --format='%ae' HEAD").trim()
      mail body: "project build error is here: ${env.BUILD_URL}" ,
      from: 'hydro-support@provectus.com',
      replyTo: 'noreply@provectus.com',
      subject: 'project build failed',
      to: gitEmail
      throw err
    }
  }
}

def test_mist(sparkVersion) {
    echo 'prepare for Mist with Spark version - ' + sparkVersion
    def mosquitto = docker.image('ansi/mosquitto:latest').run()
    def hdfs = docker.image('hydrosphere/hdfs:experimental').run("start")
    echo 'Testing Mist with Spark version: ' + sparkVersion
    def mistId = sh(returnStdout: true, script: "docker create --link ${mosquitto.id}:mosquitto --link ${hdfs.id}:hdfs 060183668755.dkr.ecr.eu-central-1.amazonaws.com/mist:tests-${sparkVersion} tests").trim()
      sh "docker cp ${env.WORKSPACE}/. ${mistId}:/usr/share/mist"
      sh "docker start ${mistId}"
      sh "docker logs -f ${mistId}"

    def checkExitCode = sh(script: "docker inspect -f {{.State.ExitCode}} ${mistId}", returnStdout: true).trim()
    echo "Build flag: ${checkExitCode}"
    if ( checkExitCode == "1" ) {
          sh "docker rm -f ${mistId}"
          echo 'remove containers'
          mosquitto.stop()
          mistVolume.stop()
          hdfs.stop()
          error("Tests failed")
    }
    sh "docker rm -f ${mistId}"

    echo 'remove containers'
    mosquitto.stop()
    mistVolume.stop()
    hdfs.stop()

    def unusedVolumes = sh(script: "docker volume ls -qf dangling=true", returnStdout: true)
    sh "docker volume rm ${unusedVolumes}"
}

def build_image(sparkVersion) {
  docker.withRegistry('https://index.docker.io/v1/', '2276974e-852b-45ab-bf14-9136e1b31217') {
    echo 'Building Mist with Spark version: ' + sparkVersion
    def mistImg = docker.build("hydrosphere/mist:${env.BRANCH_NAME}-${sparkVersion}", "--build-arg SPARK_VERSION=${sparkVersion} .")
    echo 'Pushing Mist with Spark version: ' + sparkVersion
    mistImg.push()
  }
}
