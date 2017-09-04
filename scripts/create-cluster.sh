#!/usr/bin/env bash
set -e
set -x

KEYNAME=${1? "Must provide KeyName parameter"}

CLUSTERID=$(aws emr create-cluster --name "Spark sessionization" --release-label emr-5.8.0 \
  --applications Name=Spark Name=Ganglia \
  --instance-type m4.xlarge \
  --instance-count 4 \
  --use-default-roles \
  --ec2-attributes KeyName=$KEYNAME,SubnetId=subnet-c434a8a3,AdditionalMasterSecurityGroups=sg-49a3e531| grep -o 'j-\w*')
