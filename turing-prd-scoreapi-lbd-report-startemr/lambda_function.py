from start_emr import TuringEMRStepJobs
from datetime import datetime

current_date = datetime.today().strftime('%Y-%m-%d')


def lambda_handler(event, context):
    event_region = event['region']

    response = TuringEMRStepJobs(cluster_name='ETL - Relatorio Consolidacao API - Via Varejo',
                                 region=event_region,
                                 bucket='bkt-api-viavarejo-prd',
                                 parameters_step_jobs={
                                     'path_base_stage': 'report/stage',
                                     'path_base_target': 'report/target',
                                     'path_base_setup': 'setup/report'
                                 })
    print(response)
