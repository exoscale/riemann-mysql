@Library('jenkins-pipeline') _

node {
  // Wipe the workspace so we are building completely clean
  cleanWs()

  try {
    dir('src') {
      stage('checkout code') {
        checkout scm
      }
      stage('build') {
        gitBuildPackage('xenial')
      }
    }
    stage('upload') {
      //aptlyUpload('staging', 'xenial', 'main', 'build-area/*xenial*deb')
    }
  }
  catch (err) {
    currentBuild.result = 'FAILURE'
    ircNotification()
    throw err
  }
  finally {
    if (currentBuild.result != 'FAILURE') {
      ircNotification()
    }
  }
}
