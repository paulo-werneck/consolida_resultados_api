import boto3
from pyspark.sql import SparkSession
from sys import argv
import s3fs
import json
import re

spark = SparkSession.builder.appName('ETL_Consolidacao_results_API-Via_Varejo').getOrCreate()
s3_fs = s3fs.S3FileSystem()
athena = boto3.client('athena')
s3 = boto3.resource('s3')


class APIdataIngestion:

    def __init__(self):
        self.path_metadata_json = argv[1]
        self.bucket = argv[2]
        self.path_base_stage = argv[3]
        self.path_base_target = argv[4]
        self.current_date = argv[5]

        with open(self.path_metadata_json, 'r') as f:
            att = json.load(f)

        self.database = att.get('metadata').get('database')
        self.table = att.get('metadata').get('table')
        self.fields = att.get('fields')

        if not self.table or not self.table.startswith('tb'):
            raise Exception("'table' não informado ou fora do padrão de nomenclatura no json. Ex: tb_nomedomodelo")

        if not self.database:
            raise Exception("'database' não informado no json")

        if not self.fields:
            raise Exception("'fields' não informados no json")
        else:
            fields_name = [x.get('name') for x in self.fields]
            if 'dt_particao' not in fields_name:
                raise Exception("'dt_particao' não informado na sessão de 'fields'")
            elif 'dth_ingestao_arquivo' not in fields_name:
                raise Exception("'dth_ingestao_arquivo' não informado na sessão de 'fields'")

    def get_file_paths_to_process(self):
        """ Get files to process in stage """

        lst_paths_final = []
        model_name = '_'.join(self.table.split('_')[1:-1])
        type_table = self.table.split('_')[-1]
        lst_paths = [x[0] for x in s3_fs.walk(f's3://{self.bucket}/{self.path_base_stage}/{model_name}/')]
        for path in lst_paths:
            if re.search("(....)-(\d\d)-(\d\d)$", path) and str(self.current_date) not in path and type_table in path:
                lst_paths_final.append(path)
        return lst_paths_final

    @staticmethod
    def casting_fields(data_frame, schema_casting):
        """ casting fields of input files """

        return data_frame.select([
            data_frame[schema.get('name')].cast(schema.get('type')).alias(schema.get('alias'))
            for schema in schema_casting
        ])

    def process_files(self):
        """ Process files found in stage and ingestion it in target converted in parquet """

        for path in self.get_file_paths_to_process():
            df = spark.read.csv(path=f's3://{path}/', header=True, inferSchema=True)
            df = self.casting_fields(data_frame=df, schema_casting=self.fields)

            path_output = f's3://{self.bucket}/{self.path_base_target}/{self.database}/{self.table}/'
            df.repartition(1).write.mode("append").partitionBy("dt_particao").option("header", "true").parquet(path_output)

            print(f'Novas particoes ingeridas: {path.split("/")[-1]}')

    def load_athena_partitions(self):
        """ Load Athena table partitions """

        athena.start_query_execution(
            QueryString=f'MSCK REPAIR TABLE {self.database}.{self.table}',
            QueryExecutionContext={'Database': self.database},
            ResultConfiguration={'OutputLocation': f's3://{self.bucket}/logs/Athena/'})
        print(f'Novas particoes do Athena foram carregadas na tabela: {self.table}')

    def remove_old_files_from_stage(self):
        """ Remove processed files from stage area """

        s3_bucket = s3.Bucket(self.bucket)
        for file in self.get_file_paths_to_process():
            s3_bucket.objects.filter(Prefix=file.replace(self.bucket + '/', '')).delete()
            print(f'Arquivos deletados da area de stage {file}')

    def main(self):
        print("Iniciando Ingestoes ...")
        print("Obtendo arquivos/particoes da area de stage para processamento ...")
        print()
        files = self.get_file_paths_to_process()
        print("Lista de arquivos/particoes a serem processadas:")
        for i in files:
            print(f"- {i}")
        self.process_files()
        self.load_athena_partitions()
        self.remove_old_files_from_stage()


if __name__ == '__main__':
    x = APIdataIngestion()
    x.main()
