parallel (
    Spark_1_5_2 : {
      test_mist("JenkinsOnDemand","1.5.2")
    },
    Spark_1_6_2 : {
      test_mist("JenkinsOnDemand","1.6.2")
    },
    Spark_2_0_2 : {
      test_mist("JenkinsOnDemand","2.0.2")
    },
    Spark_2_1_0 : {
      test_mist("JenkinsOnDemand","2.1.0")
    }
)

node("JenkinsOnDemand") {
    def tag = sh(returnStdout: true, script: "git tag -l --contains HEAD").trim()
    if (tag.startsWith("v")) {
        stage('Publish in Maven') {
            sh "${env.WORKSPACE}/sbt/sbt 'set pgpPassphrase := Some(Array())' mistLibSpark1/publishSigned"
            sh "${env.WORKSPACE}/sbt/sbt mistLibSpark1/sonatypeRelease"

            sh "${env.WORKSPACE}/sbt/sbt 'set pgpPassphrase := Some(Array())' mistLibSpark2/publishSigned"
            sh "${env.WORKSPACE}/sbt/sbt mistLibSpark2/sonatypeRelease"
        }
    }
}

def test_mist(slaveName,sparkVersion) {
  node(slaveName) {
    wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'XTerm']) {
      try {
        stage('Clone project ' + sparkVersion) {
          checkout scm
          sh "cd ${env.WORKSPACE}"
        }

        stage('Build and test') {
          echo 'Testing Mist with Spark version: ' + sparkVersion
          sh "${env.WORKSPACE}/sbt/sbt -DsparkVersion=${sparkVersion} testAll"
        }

        def tag = sh(returnStdout: true, script: "git tag -l --contains HEAD").trim()
        if (tag.startsWith("v")) {
            stage('Publish in DockerHub') {
              sh "${env.WORKSPACE}/sbt/sbt -DsparkVersion=${sparkVersion} mist/dockerBuildAndPush"
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
