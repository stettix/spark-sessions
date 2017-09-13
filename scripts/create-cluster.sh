#!/usr/bin/env bash
set -e
set -x

KEYNAME=${1? "Must provide KeyName parameter"}

CLUSTERID=$(aws emr create-cluster --name "Spark sessionization" --release-label emr-5.8.0 \
  --tags "Name=Spark Sessionization" \
  --applications Name=Spark Name=Hadoop Name=Ganglia \
  --instance-groups '
[
  {
    "Name": "Driver",
    "InstanceCount": 1,
    "InstanceGroupType": "MASTER",
    "InstanceType": "m4.4xlarge",
    "EbsConfiguration": {
      "EbsBlockDeviceConfigs": [
        {
          "VolumeSpecification": {
            "SizeInGB": 128,
            "VolumeType": "gp2"
          },
          "VolumesPerInstance": 1
        }
      ]
    },
    "BidPrice": "0.25"
  },
  {
    "Name": "Worker nodes",
    "InstanceGroupType": "CORE",
    "InstanceCount": 4,
    "InstanceType": "m4.4xlarge",
    "EbsConfiguration": {
      "EbsBlockDeviceConfigs": [
        {
          "VolumeSpecification": {
            "SizeInGB": 128,
            "VolumeType": "gp2"
          },
          "VolumesPerInstance": 1
        }
      ]
    },
    "BidPrice": "0.25"
  }
]' \
  --log-uri 's3://logs.multe.co.uk/elasticmapreduce/' \
  --enable-debugging \
  --use-default-roles \
  --configurations file://./spark-configs.json \
  --ec2-attributes KeyName=$KEYNAME,SubnetId=subnet-c434a8a3,AdditionalMasterSecurityGroups=sg-49a3e531| grep -o 'j-\w*')
