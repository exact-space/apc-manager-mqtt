pipeline {
    agent any  
    environment {
        VERSION = "${env.BUILD_ID}"
        registry = "dev.exactspace.co"
        APP_NAME = "apc-manager-mqtt-es"
    }
    stages {
        stage("get scm") {
            steps {
                checkout([$class: 'GitSCM',
                    branches: [[name: 'main']],
                    userRemoteConfigs: [[url: 'git@github.com:exact-space/apc-manager-mqtt.git']]
                ])
            }
        }
        stage("cython compilation") {
            steps {
                sh "cython index.py --embed"
            }
        }
        stage("gcc") {
            steps {
                sh "gcc -I/usr/include/python2.7 -Wall -Wextra -O2 -g -o index index.c -lpython2.7"
            }
        }
        /*stage("sending Build Approval mail to Devops Team") { 
            steps {
                script {
                    def mailAddresses = readFile("${env.WORKSPACE}/devops-mail.txt").trim().split("\\s*,\\s*")
                    if (mailAddresses) {
                        emailext body: "Please Proceed or Abort the building of image for ${currentBuild.fullDisplayName}", 
                            subject: "Build Approval for ${currentBuild.fullDisplayName}", 
                            to: mailAddresses.join(',')
                    }
                }
            }
        }
        stage("NEED APPROVAL TO Build the Image"){
            steps {
                input message: "Build the new image ?"
            }
        }*/
        stage("building images") {
            steps {
                sh "sudo docker build --rm --no-cache -t $APP_NAME:r1 ."
            }
        }
        stage("tagging images-r1") {
            steps {
                sh "sudo docker tag $APP_NAME:r1 $registry/$APP_NAME:r1"
            }
        }
        stage("remove old docker image-r1") {
            steps {
                sh "sudo docker image rm $APP_NAME:r1"
            }
        }
        stage("image push-r1") {
            steps {
                sh "sudo docker push $registry/$APP_NAME:r1"
            }
        }    
        stage('deploying to prod') {
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                timeout(time: 15, unit: 'MINUTES') {
                    httpRequest url: 'https://data.exactspace.co/deployservice/cicd/mqtt/apc-manager-mqtt', timeout: 900000
                }
            }
        }
        }
        stage('deploying to UTCL') {
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                timeout(time: 15, unit: 'MINUTES') {
                    httpRequest url: 'https://cpp.utclconnect.com/deployservice/cicd/mqtt/apc-manager-mqtt', timeout: 900000
                }
            }
        }
        }
        stage('deploying to thermax') {
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                timeout(time: 15, unit: 'MINUTES') {
                    httpRequest url: 'https://edgelive.thermaxglobal.com/deployservice/cicd/mqtt/apc-manager-mqtt', timeout: 900000
                }
            }
        }
        }
        stage('deploying to HRD') {
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                timeout(time: 15, unit: 'MINUTES') {
                    httpRequest url: 'https://hrd-dcs/deployservice/cicd/api/apc-manager-mqtt', timeout: 900000
                }
            }
        }
        }
        stage('deploying to LPG') {
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                timeout(time: 15, unit: 'MINUTES') {
                    httpRequest url: 'https://lpg-dcs/deployservice/cicd/api/apc-manager-mqtt', timeout: 900000
                }
            }
        }
        }
        stage('deploying to BHEL') {
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                timeout(time: 15, unit: 'MINUTES') {
                    httpRequest url: 'https://rmds.bhel.in/deployservice/cicd/mqtt/apc-manager-mqtt', timeout: 900000
                }
            }
        }
        }
    }
    post {
        failure {
            script {
                def mailAddresses = readFile("${env.WORKSPACE}/devops-mail.txt").trim().split("\\s*,\\s*")
                if (mailAddresses) {
                    emailext body: '${BUILD_LOG, maxLines=1000, escapeHtml=false}', 
                        subject: "deployment failed for ${currentBuild.fullDisplayName}", 
                        to: mailAddresses.join(',')
                }
            }
        }
    } 
}  
