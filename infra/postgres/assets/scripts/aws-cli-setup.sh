#!/bin/bash
export DEBIAN_FRONTEND=noninteractive 
apt-get -y install awscli
runuser -l sshuser -c "aws configure set aws_access_key_id xyz"
runuser -l sshuser -c "aws configure set aws_secret_access_key aaa"
runuser -l sshuser -c "aws configure set region us-east-1"
