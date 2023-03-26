import airflow
import json
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor


def retrieve_s3_files(**kwargs):
    data = kwargs['dag_run'].conf
    datastr = json.dumps(data)
    kwargs['ti'].xcom_push(key='data_for_emr', value=datastr)


DEFAULT_ARGS = {
    'owner': 'diana',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 10),
    'email': ['airflow_data_eng@wcd.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    dag_id='midterm_dag',
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(minutes=60),
    schedule_interval = None
)

parse_request = PythonOperator(
    task_id = 'parse_request',
    provide_context = True, # Airflow will pass a set of keyword arguments that can be used in your function
    python_callable = retrieve_s3_files,
    dag = dag
)

SPARK_STEPS = [
    {
        "Name": "Midterm_step_v01",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit","s3://dianaawsbucketwcd1/code/midterm_main_v2.py", 
                        "{{ ti.xcom_pull(task_ids='parse_request',key='data_for_emr') }}"
                    ],
        },
    }
]

JOB_FLOW_PARAMS = {
    "Name": "MidtermJF",
    "ReleaseLabel": "emr-6.3.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "Steps": SPARK_STEPS,
    "LogUri": "s3://logs-bucket-dwcd/midterm/",
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}
# the following will create a new EMR cluster for the job and then terminate it
task_emr = EmrCreateJobFlowOperator(
    task_id='task_emr',
    aws_conn_id='aws_default',
    job_flow_overrides=JOB_FLOW_PARAMS,
    dag=dag
)
# the sensor waits for the previous task to complete and receives the status from the EMR
wait_for_completion = EmrJobFlowSensor(
    task_id='wait_for_completion',
    job_flow_id="{{ task_instance.xcom_pull('task_emr', key='return_value') }}",
    aws_conn_id='aws_default',
    dag=dag
)

parse_request >> task_emr >> wait_for_completion
