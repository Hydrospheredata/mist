def versions = [
  ["2.2.0", "2.11.12"],
  ["2.3.0", "2.11.12"],
  ["2.4.0", "2.11.12"],
  ["2.4.0", "2.12.7"],
]

def branches = [:]

versions.each{ v ->
    def spark = v.get(0)
    def scala = v.get(1)
    branches["Spark_${spark}_Scala${scala}"] = {
        test_mist("JenkinsOnDemand", spark, scala)
    }
}

//Execute test and builds in parallel
parallel branches

def publishDocs() {
    return "master".equalsIgnoreCase(env.BRANCH_NAME)
}
//some commit
node("JenkinsOnDemand") {
    stage('Final stage - scm checkout') {
             // when jenkins git plugin 3.4.0 change git scm behaviour
             // so we must force it to checkout tags as was before
             // https://issues.jenkins-ci.org/browse/JENKINS-45164
             checkout scm
             sh "cd ${env.WORKSPACE}"
             sh "git fetch --tags"
             sh "${env.WORKSPACE}/sbt/sbt clean"
    }

    if (publishDocs()) {
      stage("publish docs") {
          sh "${env.WORKSPACE}/sbt/sbt docs/makeMicrosite"
          sh "jekyll build --source ${env.WORKSPACE}/docs/target/site --destination ${env.WORKSPACE}/docs/target/site/_site"
          sshagent(['hydro-site-publish']) {
            sh "scp -o StrictHostKeyChecking=no -r ${env.WORKSPACE}/docs/target/site/_site/* jenkins_publish@hydrosphere.io:publish_dir"
          }
      }
    }

    onRelease { v ->
      stage("upload tar default") {
        sh "${env.WORKSPACE}/sbt/sbt packageTar"
        tar = "${env.WORKSPACE}/target/mist-${v}.tar.gz"
        sshagent(['hydrosphere_static_key']) {
          sh "scp -o StrictHostKeyChecking=no ${tar} hydrosphere@52.28.47.238:publish_dir"
        }
      }

      stage("upload tar 2.12") {
        sh "${env.WORKSPACE}/sbt/sbt -DscalaVersion=2.12.7 packageTar"
        tar = "${env.WORKSPACE}/target/mist-${v}-scala-2.12.tar.gz"
        sshagent(['hydrosphere_static_key']) {
          sh "scp -o StrictHostKeyChecking=no ${tar} hydrosphere@52.28.47.238:publish_dir"
        }
      }

      stage('Publish in Maven') {
          sh "${env.WORKSPACE}/sbt/sbt 'set pgpPassphrase := Some(Array())' '+ mistLib/publishSigned'"
          sh "${env.WORKSPACE}/sbt/sbt 'project mistLib' 'sonatypeReleaseAll'"
          sh "${env.WORKSPACE}/sbt/sbt 'project mistLib' 'pyPublish'"
      }
      
    }
}

def test_mist(slaveName, sparkVersion, scalaVersion) {
    node(slaveName) {
        wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'XTerm']) {
            try {
                stage('Clone project ' + sparkVersion) {
                    // when jenkins git plugin 3.4.0 change git scm behaviour
                    // so we must force it to checkout tags as was before
                    // https://issues.jenkins-ci.org/browse/JENKINS-45164
                    checkout scm
                    sh "cd ${env.WORKSPACE}"
                    sh "git fetch --tags"
                }
                
                
                stage('Build and test') {
                    //Clear derby databases
                    sh "rm -rf spark-warehouse metastore_db recovery.db derby.log"
                    echo 'Testing Mist with Spark version: ' + sparkVersion
                    try{
                        sh "${env.WORKSPACE}/sbt/sbt -Dsbt.override.build.repos=true -Dsbt.repository.config=${env.WORKSPACE}/project/repositories -DscalaVersion=${scalaVersion} -DsparkVersion=${sparkVersion} clean test"
                        sh "${env.WORKSPACE}/sbt/sbt -Dsbt.override.build.repos=true -Dsbt.repository.config=${env.WORKSPACE}/project/repositories -DscalaVersion=${scalaVersion} -DsparkVersion=${sparkVersion} it:test"
                    }finally{
                        junit testResults: '**/target/test-reports/io.hydrosphere*.xml', allowEmptyResults: true
                    }
                }

                onRelease { v ->
                  stage('Publish in DockerHub') {
                      sh "${env.WORKSPACE}/sbt/sbt -DscalaVersion${scalaVersion} -DsparkVersion=${sparkVersion} docker"
                      def scalaPostfix = scalaVersion.startsWith("2.12") ? "-scala-2.12" : ""
                      def name = "hydrosphere/mist:${v}-${sparkVersion}${scalaPostfix}"
                      sh "docker push $name"
                  }
                }
            }
            catch (err) {
                currentBuild.result = "FAILURE"
                echo "${err}"
                gitEmail = sh(returnStdout: true, script: "git --no-pager show -s --format='%ae' HEAD").trim()
                mail body: "project build error is here: ${env.BUILD_URL}",
                        from: 'hydro-support@provectus.com',
                        replyTo: 'noreply@provectus.com',
                        subject: 'project build failed',
                        to: gitEmail
                throw err
            }
        }
    }
}

def onRelease(Closure body) {
  def describe = sh(returnStdout: true, script: "git describe").trim()
  if (describe ==~ /^v\d+.\d+.\d+(-RC\d+)?/)
    body(describe.replace("v", ""))
}
