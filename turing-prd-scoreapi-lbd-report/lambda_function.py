import boto3
from datetime import datetime
import pandas as pd
from pytz import timezone

s3 = boto3.client('s3')


def get_file_date(bucket, key_source):
    """ Get last modified date file """

    obj = s3.get_object(Bucket=bucket, Key=key_source)
    file_date = obj.get('LastModified').astimezone(timezone('America/Sao_Paulo'))
    return (
        datetime.strftime(file_date, '%Y-%m-%d'),
        datetime.strftime(file_date, '%Y-%m-%d %H:%M:%S')
    )


def move_files_to_stage(bucket_source, key_source, bucket_stage, key_stage):
    """ Move file from /results to /report/stage """

    s3.copy_object(
        Bucket=bucket_stage,
        Key=key_stage,
        CopySource={'Bucket': bucket_source, 'Key': key_source},
        Metadata={'name': key_stage},
        MetadataDirective='REPLACE'
    )


def insert_fields_dataset(bucket, key_stage, **kwfields):
    """ Insert new fields in file on /report/stage """

    df = pd.read_csv(f's3://{bucket}/{key_stage}')

    for field, value in kwfields.items():
        if field == 'id_transacao':
            df.insert(0, field, value)
        else:
            df[field] = value

    df.to_csv(f's3://{bucket}/{key_stage}', index=False, index_label=False)


def lambda_handler(event, context):
    event_bucket = event['Records'][0]['s3']['bucket']['name']
    event_key = event['Records'][0]['s3']['object']['key']
    event_region = event['Records'][0]['awsRegion']

    key_results_base, key_model, key_folder_id, key_file = event_key.split("/")
    key_stage_base = 'report/stage'
    date_partition, date_time = get_file_date(event_bucket, event_key)
    key_stage_full = f'{key_stage_base}/{key_model}/{key_file.split(".")[0]}/' \
                     f'{date_partition}/{key_folder_id}_{key_file}'

    if 'input.csv' in event_key or 'output.csv' in event_key:
        move_files_to_stage(bucket_source=event_bucket,
                            key_source=event_key,
                            bucket_stage=event_bucket,
                            key_stage=key_stage_full)

        insert_fields_dataset(bucket=event_bucket,
                              key_stage=key_stage_full,
                              id_transacao=key_folder_id,
                              dth_ingestao_arquivo=date_time,
                              dt_particao=date_partition
                              )

    print({'processed_file': {'model': key_model, 'id_folder': key_folder_id, 'file': key_file}})
