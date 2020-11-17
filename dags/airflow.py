from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from custom import CronSensor, PrdSensor

# Set Timezone for Schedule Interval and execution
schedule_tz = pendulum.timezone("Canada/Eastern")

# Set DAG level variables
dag_nm = 'music_etl_load'
container_name = dag_nm + "_" + "{{ ds }}"
dag_id = "{{ dag.dag_id }}"
audit_job_id = "{{ dag.dag_id }}/{{ task.task_id }}/{{ run_id }}"

env = Variable.get('env')
if env == 'dev':
    branch = 'dev'
else:
    branch = 'prd'
image = 'spark:latest'
tasks = ['songplay_table_010_STG', 'songplay_table_020_TGT', 'users_table_010_STG', 'users_table_020_TGT', 'songs_table_010_STG', 'songs_table_020_TGT']

container_name = '{{ dag.dag_id }}_{{ ds_nodash }}'
git_repo = 'dataloading_using_airflow'

print("=========================================================================================================================")
print("dag_id is: " + dag_id)
print("container_name is: " + container_name)
print("=========================================================================================================================")

# scheduling

default_args = {
    'owner': 'sbhattacharjee',
    'start_date': datetime(2020, 9, 21, 20, 0, 0, tzinfo=schedule_tz),
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'email': 'shalini.bhat29@gmail.com' if env == 'dev' else ['shalini.bhat29@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'wait_for_downstream': False
}

dag = DAG(
    dag_id=dag_nm,
    catchup=False,
    default_args=default_args,
    schedule_interval="0 8 * * *",
    orientation='TB',
    max_active_runs=1
)


etl_run = 'DELTA'
exec_time = '{{ execution_date.strftime("%Y-%m-%dT%H:%M:%S") }}'
delta_timestamp = '{{ (execution_date).in_timezone(\'Canada/Eastern\').strftime("%y-%m-%d %H:%M:%S") }}'
logical_dt = '{{ next_ds }}'
effective_timestamp = '{{ (next_execution_date).in_timezone(\'Canada/Eastern\').strftime("%Y-%m-%d %H:%M:%S") }}'

# Tasks
# ==============================================================================================================================
#

start = DummyOperator(task_id='start', dag=dag)

end = DummyOperator(
    task_id='end',
    sla=timedelta(hours=4),
    email=['shalini.bhat29@gmail.com'],
    dag=dag
)

# Stop container if it exists
stop_old_container = SSHOperator(
    ssh_conn_id='ssh_docker_server',   task_id='stop_old_container',   command=f"""docker container ls -a -q --filter "name={container_name}" | grep -q . && docker stop {container_name} && docker rm -fv {container_name}; exit 0""",   dag=dag
)


# Start the container
start_new_container = SSHOperator(
    ssh_conn_id='ssh_docker_server',   task_id='start_new_container',   command=f"docker container run -dt --rm --name {container_name} -v /home/ec2-user/.ssh:/root/.ssh {image}",   dag=dag
)


# Git clone
git_clone_repo = SSHOperator(
    ssh_conn_id='ssh_docker_server',   task_id='git_clone_repo',   command=f"docker exec {container_name} /bin/bash -c 'cd /environment;git clone -b {branch} git@github.com:SMG-Digital/{git_repo}.git'",   dag=dag
)

docker_command = {}

for task_id in tasks:
    cmd = f"docker exec {container_name} bash -c 'cd /environment/{git_repo}; spark-submit src/execute_snowflake_sql.py {env} {task_id} {exec_time} {audit_job_id} {git_repo} {logical_dt} {etl_run} {effective_timestamp} {delta_timestamp}'"
docker_command[task_id] = cmd

# Create operators for every etl

dag_op = {}
for k, v in docker_command.items():
    dag_op[k] = SSHOperator(ssh_conn_id='ssh_docker_server', dag=dag, command=v, task_id=k, trigger_rule='none_failed')


task_id = 'kill_container'

cmd_kill_container = f"docker container ls -qa --filter name={container_name} | grep -q . && (docker stop {container_name} || echo 'docker stopped') || echo 'not found'"

print("cmd_kill_container: " + cmd_kill_container)

kill_container = SSHOperator(
    ssh_conn_id='ssh_docker_server',
    task_id=task_id,
    command=cmd_kill_container,
    dag=dag,
    trigger_rule='none_failed')

start >> stop_old_container >> start_new_container >> git_clone_repo
git_clone_repo >> dag_op['songplay_table_010_STG'] >> dag_op['songplay_table_020_TGT'] >> kill_container
git_clone_repo >> dag_op['users_table_010_STG'] >> dag_op['users_table_020_TGT'] >> kill_container
git_clone_repo >> dag_op['songs_table_010_STG'] >> dag_op['songs_table_020_TGT'] >> kill_container
kill_container >> end
