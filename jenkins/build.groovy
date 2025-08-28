pipeline {
    agent {
        kubernetes {
            workspaceVolume dynamicPVC(accessModes: 'ReadWriteOnce', requestsSize: "50Gi", storageClassName: "standard-rwo")
            yaml containers.withContainers([
                    "kaniko",
                    "containerdiff",
                    "utility",
                    "helm",
                    "jdkDockerBuilder(maven-8)"
            ])
        }
    }
    options{
      buildDiscarder(logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '', daysToKeepStr: '14', numToKeepStr: '28'))
      timestamps()
      disableConcurrentBuilds()
    }
    environment {
      // The largest duration of the build before an alert is sent to the slow
      // builds channel for investigation, set in milliseconds
      ALERT_DURATION='600000'
      SLOW_SLACK_CHANNEL='#fusion-slow-builds'

      // URL to GitHub PR page or current commit
      GITHUB_COMMIT_URL = "${env.GIT_BRANCH =~/^PR.*merge$/ ?\
          "${env.GITHUB_BASE_URL}/pull/${env.GIT_BRANCH - ~/^PR-/ - ~/-merge/}" :\
          "${env.GITHUB_BASE_URL}/commit/${env.GIT_COMMIT}"}"



      SLACK_CHANNEL='#fusion-builds'
      SLACK_FAILURE_CHANNEL='#platform-team'

      // Slack message colours
      COLOUR_STARTED = '#2881c9'
      COLOUR_PASSED  = '#00ae42'
      COLOUR_FAILED  = '#ff0000'

    }
    parameters {
        string(defaultValue: "", name: 'USE_GIT_BRANCH', description: 'The git branch to use')
    }

    stages {
        stage("Git Checkout") {
            steps {
                slackSend (message: "Build spark-solr release process started", \
                    channel: env.SLACK_CHANNEL, color: env.COLOUR_STARTED)

                checkout([$class: 'GitSCM',
                        branches: [[name: params.USE_GIT_BRANCH]],
                        doGenerateSubmoduleConfigurations: false,
                        extensions: [],
                        gitTool: 'Default',
                        submoduleCfg: [],
                        userRemoteConfigs: [[credentialsId: '1d4dd6a4-2e4f-4bb3-91ad-bac4054d9d45', refspec: "+refs/heads/*:refs/remotes/origin/*", url: 'git@github.com:lucidworks/spark-solr.git']]])
            }
        }
        stage("Build") {
            steps {
                script {
                    withCredentials([
                      [$class: 'FileBinding', credentialsId: 'spark-solr-pubring', variable: 'PUBRING'],
                      [$class: 'FileBinding', credentialsId: 'spark-solr-secring', variable: 'SECRING']
                    ]){
                        dir("${env.WORKSPACE}"){
                          docker.withRegistry('https://fusion-dev-docker.ci-artifactory.lucidworks.com', 'ARTIFACTORY_JENKINS') {
                              docker.image('fusion-dev-docker.ci-artifactory.lucidworks.com/maven-docker-builder:v0.0.1').inside("-v ${env.WORKSPACE}:/root -w /root") {
                                sh """
                                  mkdir -p gnupg
                                  cp ${PUBRING} gnupg/pubring.gpg
                                  cp ${SECRING} gnupg/secring.gpg
                                  chmod go-rwx gnupg
                                  mvn --version
                                  mvn clean package -DskipTests
                                """
                            }
                        }
                    }
                }
            }
          }
          post {
            success {

                  archiveArtifacts 'target/*.jar'
            }
          }
        }
    }
    post {
        success {
            slackSend (message: "Build <${env.BUILD_URL}|#${env.BUILD_NUMBER}> spark-solr release passed " +\
                "in ${currentBuild.durationString - ~/and counting/}", \
                channel: env.SLACK_CHANNEL, color: env.COLOUR_PASSED)
        }
        failure {
            slackSend (message: "Build <${env.BUILD_URL}|#${env.BUILD_NUMBER}> spark-solr release failed " +\
                "in ${currentBuild.durationString - ~/and counting/}", \
                channel: env.SLACK_CHANNEL, color: env.COLOUR_FAILED)
        }
    }

}