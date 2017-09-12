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
    "InstanceCount": 1,
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
    "InstanceGroupType": "MASTER",
    "InstanceType": "m4.4xlarge",
    "Name": "Driver",
    "BidPrice": "0.20"
  },
  {
    "InstanceCount": 4,
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
    "InstanceGroupType": "CORE",
    "InstanceType": "m4.4xlarge",
    "Name": "Worker nodes",
    "BidPrice": "0.20"
  }
]' \
  --log-uri 's3://logs.multe.co.uk/elasticmapreduce/' \
  --enable-debugging \
  --use-default-roles \
  --configurations file://./spark-configs.json \
  --ec2-attributes KeyName=$KEYNAME,SubnetId=subnet-c434a8a3,AdditionalMasterSecurityGroups=sg-49a3e531| grep -o 'j-\w*')
