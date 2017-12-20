//Spark Version
versions = [
        "2.0.0",
        "2.1.0",
        "2.2.0"
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

def publishDocs() {
    return "feature/jekill_docs".equalsIgnoreCase(env.BRANCH_NAME)
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

    def tag = sh(returnStdout: true, script: "git tag -l --contains HEAD").trim()
    if (tag.startsWith("v")) {
        v = tag.replace("v", "")
        stage("upload tar") {
          sh "${env.WORKSPACE}/sbt/sbt packageTar"
          tar = "${env.WORKSPACE}/target/mist-${v}.tar.gz"
          sshagent(['hydrosphere_static_key']) {
            sh "scp -o StrictHostKeyChecking=no ${tar} hydrosphere@52.28.47.238:publish_dir"
          }
        }

        stage('Publish in Maven') {
            sh "${env.WORKSPACE}/sbt/sbt 'set pgpPassphrase := Some(Array())' mistLib/publishSigned"
            sh "${env.WORKSPACE}/sbt/sbt 'project mistLib' 'sonatypeRelease'"
        }
    }
}

def test_mist(slaveName, sparkVersion) {
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
                    sh "rm -rf metastore_db recovery.db derby.log"
                    echo 'Testing Mist with Spark version: ' + sparkVersion
                    try{
                        sh "${env.WORKSPACE}/sbt/sbt -Dsbt.override.build.repos=true -Dsbt.repository.config=${env.WORKSPACE}/project/repositories -DsparkVersion=${sparkVersion} clean test"
                        sh "${env.WORKSPACE}/sbt/sbt -Dsbt.override.build.repos=true -Dsbt.repository.config=${env.WORKSPACE}/project/repositories -DsparkVersion=${sparkVersion} it:test"
                    }finally{
                        junit testResults: '**/target/test-reports/io.hydrosphere*.xml', allowEmptyResults: true
                    }
                }

                def tag = sh(returnStdout: true, script: "git tag -l --contains HEAD").trim()
                if (tag.startsWith("v")) {
                    version = tag.replace("v", "")
                    stage('Publish in DockerHub') {
                        sh "${env.WORKSPACE}/sbt/sbt -DsparkVersion=${sparkVersion} dockerBuildAndPush"
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
