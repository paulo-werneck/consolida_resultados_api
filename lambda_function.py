import boto3
from datetime import datetime
import pandas as pd
from start_emr import TuringEMRStepJobs

s3 = boto3.client('s3')
current_date = datetime.today().strftime('%Y%m%d')


def move_files_to_stage(bucket_source, key_source, bucket_stage, key_stage):
    """ Move file from /results to /report/stage """

    s3.copy_object(
        Bucket=bucket_stage,
        Key=key_stage,
        CopySource={'Bucket': bucket_source, 'Key': key_source},
        Metadata={'name': key_stage},
        MetadataDirective='REPLACE'
    )


def insert_id_and_partition(bucket, key_stage, key_folder_id):
    """ Insert partition and id in file on /report/stage """

    df = pd.read_csv(f's3://{bucket}/{key_stage}')
    df.insert(0, 'id_transacao', key_folder_id)
    df['dt_particao'] = current_date
    df.to_csv(f's3://{bucket}/{key_stage}', index=False, index_label=False)


def check_files_to_process(bucket, prefix):
    """ Return True if there are old files to process """

    response = s3.list_objects(
        Bucket=bucket,
        Prefix=prefix
    )

    if response.get('Contents'):
        if current_date not in response.get('Contents')[0].get('Key'):
            return True
    return False


def lambda_handler(event, context):
    event_bucket = event['Records'][0]['s3']['bucket']['name']
    event_key = event['Records'][0]['s3']['object']['key']
    event_region = event['Records'][0]['awsRegion']

    key_results_base, key_model, key_folder_id, key_file = event_key.split("/")
    key_stage_base = 'report/stage'
    key_stage_full = f'{key_stage_base}/{key_model}/{key_file.split(".")[0]}/{current_date}/{key_folder_id}_{key_file}'

    if 'input.csv' in event_key or 'output.csv' in event_key:
        move_files_to_stage(bucket_source=event_bucket,
                            key_source=event_key,
                            bucket_stage=event_bucket,
                            key_stage=key_stage_full)

        insert_id_and_partition(bucket=event_bucket,
                                key_stage=key_stage_full,
                                key_folder_id=key_folder_id)

        obj_process = check_files_to_process(bucket=event_bucket,
                                             prefix=f'{key_stage_base}/{key_model}/{key_file.split(".")[0]}/')

        if obj_process:
            TuringEMRStepJobs(cluster_name='ETL_Consolidacao_results_API-Via_Varejo',
                              region=event_region,
                              bucket=event_bucket,
                              step_job_parameters={
                                  'stage_base': key_stage_base,
                                  'path_target': 'report/target',
                                  'database': 'turing_prd_scoreapi_usage',
                                  'current_date': current_date,
                              })

    return {'processed_file': {'model': key_model, 'id_folder': key_folder_id, 'file': key_file}}
