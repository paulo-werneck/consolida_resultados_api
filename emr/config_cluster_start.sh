#!/bin/bash -xe

sudo yum -y install htop
sudo pip-3.6 install s3fs boto3
sudo pip-3.6 install --upgrade six

PATH_JOB=/tmp/emr_step_job/
CLUSTER_ID=$(cat /mnt/var/lib/info/job-flow.json | jq -r ".jobFlowId")
CURRENT_DATE=$(date +%Y-%m-%d)
BUCKET=$1
PATH_BASE_STAGE=$2
PATH_BASE_TARGET=$3
PATH_BASE_SETUP=$4


mkdir -p $PATH_JOB

cd $PATH_JOB

aws s3 cp --recursive s3://$BUCKET/$PATH_BASE_SETUP/athena/fields_mapping/ .

aws s3 cp s3://$BUCKET/$PATH_BASE_SETUP/emr/step_jobs/step_job01_etl_consolidacao_api.zip .

unzip step_job01_etl_consolidacao_api.zip

for i in $(ls $PATH_JOB/*.json);
do
  aws emr add-steps --cluster-id $CLUSTER_ID --steps Type=Spark,Name="SPARK - ETL Consolidacao requisicoes API - $(ls $i | cut -d. -f2)",ActionOnFailure=CONTINUE,Args=[--deploy-mode,client,--supervise,--conf,spark.pyspark.python=/usr/bin/python3.6,file://$PATH_JOB/step_job01_etl_consolidacao_api.py,$i,$BUCKET,$PATH_BASE_STAGE,$PATH_BASE_TARGET,$CURRENT_DATE]
done