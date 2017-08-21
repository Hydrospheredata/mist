//Spark Version
versions = [
        "1.5.2",
        "1.6.2",
        "2.0.2",
        "2.1.1"
]

def branches = [:]
for (int i = 0; i < versions.size(); i++) { //TODO switch to each after JENKINS-26481
    def ver = versions.get(i)
    branches["Spark_${ver}"] = {
        test_mist("JenkinsOnDemand", ver.toString())
    }
}

//Execute test and builds in parallel
parallel branches

//some commit
node("JenkinsOnDemand") {
    stage('Final stage - scm checkout') {
        checkout scm
        sh "cd ${env.WORKSPACE}"
    }

    def tag = sh(returnStdout: true, script: "git tag -l --contains HEAD").trim()

    if (tag.startsWith("v")) {
        stage('Publish in Maven') {
            publishVersions = ["1.5.2", "2.1.0"]
            for(int i = 0; i < publishVersions.size(); i++) {
              def v = publishVersions.get(i)
              sh "${env.WORKSPACE}/sbt/sbt -DsparkVersion=${v} 'set pgpPassphrase := Some(Array())' mistLib/publishSigned"
              sh "${env.WORKSPACE}/sbt/sbt -DsparkVersion=${v} 'project mistLib' 'sonatypeRelease'"
            }
        }
    }
}

def test_mist(slaveName, sparkVersion) {
    node(slaveName) {
        wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'XTerm']) {
            try {
                stage('Clone project ' + sparkVersion) {
                    checkout scm
                    sh "cd ${env.WORKSPACE}"
                }

                stage('Build and test') {
                    //Clear derby databases
                    sh "rm -rf metastore_db recovery.db derby.log"
                    echo 'Testing Mist with Spark version: ' + sparkVersion
                    sh "${env.WORKSPACE}/sbt/sbt -Dsbt.override.build.repos=true -Dsbt.repository.config=${env.WORKSPACE}/project/repositories -DsparkVersion=${sparkVersion} clean assembly testAll"
                }

                def tag = sh(returnStdout: true, script: "git tag -l --contains HEAD").trim()
                if (tag.startsWith("v")) {
                    version = tag.replace("v", "")
                    stage('Publish in DockerHub') {
                        sh "${env.WORKSPACE}/sbt/sbt -DsparkVersion=${sparkVersion} mist/dockerBuildAndPush"
                    }

                    stage("upload tar") {
                      sh "${env.WORKSPACE}/sbt/sbt -DsparkVersion=${sparkVersion} mist/packageTar"
                      tar = "${env.WORKSPACE}/target/mist-${version}-${sparkVersion}.tar.gz"
                      sshagent(['hydrosphere_static_key']) {
                        sh "scp -o StrictHostKeyChecking=no ${tar} hydrosphere@52.28.47.238:publish_dir"
                      }
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
