parallel (
    Spark_1_5_2 : {
      test_mist("aws-slave-01","1.5.2")
    },
    Spark_1_6_2 : {
      test_mist("aws-slave-02","1.6.2")
    },
    Spark_2_1_0 : {
      test_mist("aws-slave-03","2.1.0")
    }
)

def test_mist(slaveName,sparkVersion) {
  node(slaveName) {
    wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'XTerm']) {
      try {
        stage('Clone project ' + sparkVersion) {
          checkout scm
        }

        def tag = sh(returnStdout: true, script: "git tag -l --contains HEAD").trim()

        stage('Build project example for HDFS') {
          echo "Building examples with Spark version: " + sparkVersion
          sh "cd ${env.WORKSPACE} && ${env.WORKSPACE}/sbt/sbt -DsparkVersion=${sparkVersion} 'project examples' package"
        }

        stage('Build and test') {
          echo 'prepare for Mist with Spark version - ' + sparkVersion
          def mosquitto = docker.image('ansi/mosquitto:latest').run()
          def hdfs = docker.image('hydrosphere/hdfs:latest').run(" -v ${env.WORKSPACE}:/usr/share/mist -e SPARK_VERSION=${sparkVersion}","start")
          echo 'Testing Mist with Spark version: ' + sparkVersion
          def mistId = sh(returnStdout: true, script: "docker create --link ${mosquitto.id}:mosquitto --link ${hdfs.id}:hdfs hydrosphere/mist:tests-${sparkVersion} tests").trim()
            sh "docker cp ${env.WORKSPACE}/. ${mistId}:/usr/share/mist"
            sh "docker start ${mistId}"
            sh "docker logs -f ${mistId}"

          def checkExitCode = sh(script: "docker inspect -f {{.State.ExitCode}} ${mistId}", returnStdout: true).trim()
          echo "Build flag: ${checkExitCode}"
          if ( checkExitCode == "1" ) {
                sh "docker rm -f ${mistId}"
                echo 'remove containers'
                mosquitto.stop()
                hdfs.stop()
                error("Tests failed")
          }
          sh "docker rm -f ${mistId}"

          echo 'remove containers'
          mosquitto.stop()
          hdfs.stop()
        }

        if (tag == 'release') {
          stage('Public in Maven') {
            sh "sbt publishSigned -DsparkVersion=${sparkVersion}"
            sh "sbt sonatypeRelease"
          }

          stage('Public in DockerHub') {
            build_image(sparkVersion)
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
}

def build_image(sparkVersion) {
  docker.withRegistry('https://index.docker.io/v1/', '2276974e-852b-45ab-bf14-9136e1b31217') {
    echo 'Building Mist with Spark version: ' + sparkVersion
    def mistImg = docker.build("hydrosphere/mist:${env.BRANCH_NAME}-${sparkVersion}", "--build-arg SPARK_VERSION=${sparkVersion} .")
    echo 'Pushing Mist with Spark version: ' + sparkVersion
    mistImg.push()
  }
}
