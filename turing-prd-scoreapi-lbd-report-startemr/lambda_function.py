from start_emr import TuringEMRStepJobs
from datetime import datetime

current_date = datetime.today().strftime('%Y%m%d')


def lambda_handler(event, context):
    event_region = event['region']

    response = TuringEMRStepJobs(cluster_name='ETL_Consolidacao_results_API-Via_Varejo',
                                 region=event_region,
                                 bucket='bkt-api-viavarejo-prd',
                                 step_job_parameters={
                                     'stage_base': 'report/stage',
                                     'path_target': 'report/target',
                                     'database': 'turing_prd_scoreapi_usage',
                                     'current_date': current_date,
                                 })
    print(response)
