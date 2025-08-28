@Library("one-platform-lib") _

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
                checkout scm
            }
        }
        
        stage("Build") {
            steps {
                script {
                    container('jdk-docker-builder') {
                        withCredentials([
                                [$class: 'UsernamePasswordMultiBinding', credentialsId: 'artifactory_jenkins', usernameVariable: 'ORG_GRADLE_PROJECT_lucidArtifactoryUsername', passwordVariable: 'ORG_GRADLE_PROJECT_lucidArtifactoryPassword']
                        ]){
                            sh """
                                mvn --version
                                mvn clean package -DskipTests
                            """
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