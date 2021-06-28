loadLibrary "as24-fizz-community-library@v0.10.4"

pipeline {

    agent none

    options {
        timestamps()
        buildDiscarder(logRotator(daysToKeepStr: '90'))
        disableConcurrentBuilds()
    }

    environment {
        AWS_DEFAULT_REGION = 'eu-west-1'

        // stable build number across restarts
        INVOKED_BUILD_NUMBER = getInvokedBuildNumber()

        // FAST credentials for maven etc
        FAST_TOKEN = getFastToken()
        FAST_USER = getFastUser()

        ZIP_LAMBDA = "emr_autoscaling.zip"
        LOCAL_ZIP_PATH = "./target/" + "$ZIP_LAMBDA"
        VERSION = "v1.0-" + "$INVOKED_BUILD_NUMBER"

        S3_ZIP_ARTIFACT_PATH = "s3://is24-data-pro-artifacts/emr/lambda_autoscaling/" + "$VERSION" + "/" + "$ZIP_LAMBDA"
        S3_ZIP_ARTIFACT_PATH_LATEST = "s3://is24-data-pro-artifacts/emr/lambda_autoscaling/latest/" + "$ZIP_LAMBDA"
    }

    stages {
        stage('Test & Package') {
            agent { node { label 'is24-data-pro-build-data-engineering' } }
            steps {
                script {
                    sh '''
                        make package
                        pwd
                        ls -la
                        ls -la ./target
                        aws s3 cp $LOCAL_ZIP_PATH $S3_ZIP_ARTIFACT_PATH
                    '''
                }
            }
        }
        stage('Release the artifact to s3 Prod') {
            when {
                beforeAgent true
                branch 'master'
            }
            agent { node { label 'is24-data-pro-build-data-engineering' } }
            steps {
                sh '''
                    aws s3 cp $S3_ZIP_ARTIFACT_PATH $S3_ZIP_ARTIFACT_PATH_LATEST
                '''
            }
        }
    }

    post {
        failure {
            script {
                if (env.BRANCH_NAME == 'master') {
                    slackSend channel: 'core-data-platform-alerts', color: 'danger',
                            message: "The pipeline <${env.BUILD_URL}|${currentBuild.fullDisplayName}> failed."
                }
            }
        }
    }
}