parallel (
    "stream 1" : {
          node('aws-slave-01') {
            wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'XTerm']) {
              def version = "1.5.2"
              try {
                stage('Clone project') {
                  checkout scm
                }

                def tag = sh(returnStdout: true, script: "git tag -l --contains HEAD").trim()

                stage('Build project example for HDFS') {
                  echo "Building examples with Spark version: " + version
                  sh "cd ${env.WORKSPACE} && ${env.WORKSPACE}/sbt/sbt -DsparkVersion=${version} 'project examples' package"
                }

                stage('Build and test') {
                  Spark: { test_mist(version) }
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
      },

      "stream 2" : {
        node('aws-slave-02') {
          wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'XTerm']) {
            def version = "1.2.2"
            try {
              stage('Clone project') {
                checkout scm
              }

              def tag = sh(returnStdout: true, script: "git tag -l --contains HEAD").trim()

              stage('Build project example for HDFS') {
                echo "Building examples with Spark version: " + version
                sh "cd ${env.WORKSPACE} && ${env.WORKSPACE}/sbt/sbt -DsparkVersion=${version} 'project examples' package"
              }

              stage('Build and test') {
                Spark: { test_mist(version) }
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
      },

      "stream 3" : {

          node('aws-slave-03') {
            wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'XTerm']) {
              def version = "2.1.0"
              try {
                stage('Clone project') {
                  checkout scm
                }

                def tag = sh(returnStdout: true, script: "git tag -l --contains HEAD").trim()

                stage('Build project example for HDFS') {
                  echo "Building examples with Spark version: " + version
                  sh "cd ${env.WORKSPACE} && ${env.WORKSPACE}/sbt/sbt -DsparkVersion=${version} 'project examples' package"
                }

                stage('Build and test') {
                  Spark: { test_mist(version) }
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
      },

)

def test_mist(sparkVersion) {
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
