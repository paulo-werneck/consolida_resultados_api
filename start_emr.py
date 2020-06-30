import boto3

# ----------------------------------------------
nm_empresa = 'Via_Varejo'
# ----- Cluster -------------------------------
InstanceTypeMaster = 'c5.9xlarge'  # c5n.9xlarge c5.9xlarge c5.18xlarge m5d.24xlarge
InstanceType = 'c5.9xlarge'
BidPrice_master = '0.90'
BidPrice_core = '0.90'
qtd_core = 0
# ----------------------------------------------
Ec2SubnetId = 'subnet-06fffc00251e3b3d3'
EmrManagedMasterSecurityGroup = 'sg-054b0af7d4e043e4c'
EmrManagedSlaveSecurityGroup = 'sg-075116996dce92d77'
# -----------------------------------------------


def TuringEMRStepJobs(cluster_name, region, bucket, step_job_parameters):
    conn_emr = boto3.client('emr', region)

    release_label = 'emr-5.23.0'
    log_uri = f's3://{bucket}/logs/EMR/clusters/'

    path_code_stepjob = f'file:///tmp/emr_step_job/step_job01_etl_consolidacao_api.py'

    steps = [
        {
            'Name': 'spark - ETL Consolidacao requisicoes score API',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', '--deploy-mode', 'client', '--supervise', '--conf',
                         'spark.pyspark.python=/usr/bin/python3.6', path_code_stepjob,
                         bucket, *step_job_parameters.values()]
            }
        }
    ]

    tags = [
        {'Key': 'Name', 'Value': cluster_name},
        {'Key': 'Customer', 'Value': nm_empresa},
        {'Key': 'Environment', 'Value': 'PROD'},
        {'Key': 'EMR', 'Value': 'Report'},
        {'Key': 'Product', 'Value': 'ScoreAPI'}
    ]

    if qtd_core >= 1:
        input_instance_groups = [
            {
                'InstanceCount': 1,
                'InstanceRole': 'MASTER',
                'InstanceType': InstanceTypeMaster,
                'Market': 'SPOT',
                'Name': 'master',
                'BidPrice': BidPrice_master
            },
            {
                'InstanceCount': qtd_core,
                'InstanceRole': 'CORE',
                'InstanceType': InstanceType,
                'Market': 'SPOT',
                'Name': 'core',
                'BidPrice': BidPrice_core
            }
        ]
    if qtd_core == 0:
        input_instance_groups = [
            {
                'InstanceCount': 1,
                'InstanceRole': 'MASTER',
                'InstanceType': InstanceTypeMaster,
                'Market': 'SPOT',
                'Name': 'master',
                'BidPrice': BidPrice_master
            }
        ]

    instances = {
        'InstanceGroups': input_instance_groups,
        'Ec2SubnetId': Ec2SubnetId,
        'EmrManagedMasterSecurityGroup': EmrManagedMasterSecurityGroup,
        'EmrManagedSlaveSecurityGroup': EmrManagedSlaveSecurityGroup,
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False
    }

    configurations = [
        {
            'Classification': 'hadoop-env',
            'Configurations': [
                {
                    "Classification": "export",
                    "Properties": {
                        "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"
                    },
                    "Configurations": []
                }
            ]
        },
        {
            'Classification': 'spark-env',
            'Configurations': [
                {
                    "Classification": "export",
                    "Properties": {
                        "JAVA_HOME": "/usr/lib/jvm/java-1.8.0",
                        "PYSPARK_PYTHON": "/usr/bin/python3.6",
                        "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3.6"
                    },
                    "Configurations": []}]
        }
    ]

    bootstrap_actions = [
        {
            'Name': 'Pacotes Python',
            'ScriptBootstrapAction':
                {
                    'Path': f's3://{bucket}/setup/consolidacao_results_api/emr/config_cluster_start.sh'
                }
        }
    ]

    applications = [{'Name': x} for x in ['Hadoop', 'Spark', 'Ganglia', 'Hive', 'JupyterHub']]
    service_role = 'EMR_DefaultRole'
    job_flow_role = 'EMR_EC2_DefaultRole'

    response = conn_emr.run_job_flow(
        Name=cluster_name,
        LogUri=log_uri,
        ReleaseLabel=release_label,
        Instances=instances,
        BootstrapActions=bootstrap_actions,
        Tags=tags,
        Steps=steps,
        Configurations=configurations,
        Applications=applications,
        VisibleToAllUsers=True,
        ServiceRole=service_role,
        JobFlowRole=job_flow_role
    )

    return response
