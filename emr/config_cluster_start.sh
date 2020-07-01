#!/bin/bash -xe
sudo yum -y install htop

sudo pip-3.6 install s3fs boto3

sudo pip-3.6 install --upgrade six

PATH_JOB=/tmp/emr_step_job/

mkdir -p $PATH_JOB

aws s3 cp s3://bkt-api-viavarejo-prd/setup/report/emr/step_jobs/step_job01_etl_consolidacao_api.zip $PATH_JOB

cd $PATH_JOB

unzip step_job01_etl_consolidacao_api.zip
