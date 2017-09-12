#!/usr/bin/env bash

set -e
set -x

CLUSTERID=${1?"Must provide Cluster ID parameter"}

FILE=s3://datasets.multe.co.uk/jars/spark-sessions-assembly-0.0.1.jar
INPUTPATH=s3://datasets.multe.co.uk/page_views_10kparts.parquet
OUTPUTPATH=s3://datasets.multe.co.uk/sessions/sortedpartitions

aws emr --region eu-west-1 add-steps --cluster-id $CLUSTERID \
    --steps Type=Spark,Name="Sessions using sorted partitions",Args=[--class,net.janvsmachine.sparksessions.SortWithinPartitionsSessions,--master,yarn,--deploy-mode,cluster,$FILE,$INPUTPATH,$OUTPUTPATH]
