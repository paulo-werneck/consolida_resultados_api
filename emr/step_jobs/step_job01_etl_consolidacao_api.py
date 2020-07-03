import boto3
from pyspark.sql import SparkSession
import logging
from sys import argv
import s3fs
#from emr.step_jobs import schemas
import schemas

spark = SparkSession.builder.appName('ETL_Consolidacao_results_API-Via_Varejo').getOrCreate()
s3_fs = s3fs.S3FileSystem()
athena = boto3.client('athena')
s3 = boto3.resource('s3')

_, bucket, path_base_stage, path_base_target, database, current_date = argv


def get_files_to_process():
    """ Get files to process in stage """

    lst_files_final = list()
    lst_files = [x for x in s3_fs.glob(f's3://{bucket}/{path_base_stage}/**')
                 if x.endswith('/input') or x.endswith('/output')]

    for file in lst_files:
        for path in s3_fs.glob(f'{file}/*'):
            if str(current_date) not in path:
                lst_files_final.append(path)
                logging.info(f'Partições encontradas para processamento: {path}')
    return lst_files_final


def casting_fields(data_frame, schema_casting):
    """ casting fields of input files """

    return data_frame.select([
        data_frame[field].cast(data_type).alias(field_alias) for field, field_alias, data_type in schema_casting
    ])


def process_files():
    """ Process files found in stage and ingestion it in target converted in parquet """

    lst_tables = set()

    for path in get_files_to_process():
        df = spark.read.csv(path=f's3://{path}/', header=True, inferSchema=True)
        if 'input' in path:
            df = casting_fields(data_frame=df, schema_casting=schemas.schema_input_file)

        table = 'tb_{model}_{file}'.format(model=path.split("/")[3], file=path.split("/")[-2])
        lst_tables.add(table)

        path_output = f's3://{bucket}/{path_base_target}/{database}/{table}/'
        df.coalesce(1).write.mode("append").partitionBy("dt_particao").option("header", "true").parquet(path_output)

        logging.info(f'Novas partições inseridas: {path.split("/")[-1]}')
    return lst_tables


def load_athena_partitions(db_name=database, lst_tables=None):
    """ Load Athena table partitions """

    for table in lst_tables:
        athena.start_query_execution(
            QueryString=f'MSCK REPAIR TABLE {db_name}.{table}',
            QueryExecutionContext={'Database': db_name},
            ResultConfiguration={'OutputLocation': f's3://{bucket}/logs/Athena/'})
        logging.info(f'Carregadas partições do Athena na tabela: {table}')


def remove_old_files_from_stage():
    """ Remove processed files from stage area """

    s3_bucket = s3.Bucket(bucket)
    for file in get_files_to_process():
        s3_bucket.objects.filter(Prefix=file.replace(bucket + '/', '')).delete()
        logging.info(f'Arquivos deletados da area de stage {file}')


if __name__ == '__main__':
    processed_files = process_files()
    load_athena_partitions(lst_tables=processed_files)
    remove_old_files_from_stage()
