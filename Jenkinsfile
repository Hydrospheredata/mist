node {
  wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'XTerm']) {

    currentBuild.result = "SUCCESS"
    try {
      stage('clone project') {
        checkout scm
      }

      def tag = sh(returnStdout: true, script: "git tag -l --contains HEAD").trim()

      stage('build and test') {
        parallel ( failFast: false,
          Spark_1_5_2: { test_mist("1.5.2") },
          Spark_1_6_2: { test_mist("1.6.2") },
          Spark_2_0_0: { test_mist("2.0.0") },
        )
      }
      if (tag == 'release') {
        stage('public in maven') {
          parallel ( failFast: false,
            Publish_1_5_2: {sh "sbt publishSigned -DsparkVersion=1.5.2" },
            Publish_2_0_0: {sh "sbt publishSigned -DsparkVersion=1.5.2" },
          )
          sh "sbt sonatypeRelease"
        }

        stage('public in docker hub') {
          parallel ( failFast: false,
            Spark_1_5_2: { build_image("1.5.2") },
            Spark_1_6_2: { build_image("1.6.2") },
            Spark_2_0_0: { build_image("2.0.0") },
          )
        }
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
    def mistVolume = docker.image("hydrosphere/mist:tests-${sparkVersion}").run("-v /usr/share/mist")
    def hdfs = docker.image('hydrosphere/hdfs:latest').run("--volumes-from ${mistVolume.id}", "start")
    echo 'Testing Mist with Spark version: ' + sparkVersion
    def mistId = sh(returnStdout: true, script: "docker create --link ${mosquitto.id}:mosquitto --link ${hdfs.id}:hdfs hydrosphere/mist:tests-${sparkVersion} tests").trim()
      sh "docker cp ${env.WORKSPACE}/. ${mistId}:/usr/share/mist"
      sh "docker start ${mistId}"
      sh "docker logs -f ${mistId}"
      sh "docker rm -f ${mistId}"

    echo 'remove containers'
    mosquitto.stop()
    mistVolume.stop()
    hdfs.stop()
}

def build_image(sparkVersion) {
  docker.withRegistry('https://index.docker.io/v1/', '2276974e-852b-45ab-bf14-9136e1b31217') {
    echo 'Building Mist with Spark version: ' + sparkVersion
    def mistImg = docker.build("hydrosphere/mist:${env.BRANCH_NAME}-${sparkVersion}", "--build-arg SPARK_VERSION=${sparkVersion} .")
    echo 'Pushing Mist with Spark version: ' + sparkVersion
    mistImg.push()
  }
}
