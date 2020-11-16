import logging
from datetime import datetime,timedelta
import sys
import json
from SnowflakeConnection import SnowflakeConnection

logging.basicConfig(level	= logging.INFO , format	= '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
					)

### if argument is 1 then it is suppose to run for testing purposes in dev environment
if len(sys.argv) == 1:
	env						= 'dev'
	task_id					= 'events_staging'
	exec_time 				= '2019-09-01T00:00:00'
	audit_job_id            = 'music_job_id_dev'
	git_repo				= 'data_music_etl'
	logical_dt				= '2019-09-01'
	effective_ts			= '2019-09-01 10:00:00'
	delta_timestamp			= '2019-08-31 10:00:00'
	etl_run   			= 'DELTA'  # DELTA for normal processing
else:
	env						= sys.argv[1]  # prd, dev 
	task_id					= sys.argv[2]  # airflow task_id needed to access parameter from config file
	exec_time 				= sys.argv[3]  # airflow task_id, required to access parame from config.json
	audit_job_id 			= sys.argv[4]
	git_repo				= sys.argv[5]
	logical_dt				= sys.argv[6]  # Airflow's {{next_ds}}
	etl_run			= sys.argv[7]
	effective_timestamp	    = sys.argv[8]  # Airflow's {{next_execution_date}}
	delta_timestamp			= sys.argv[9]  # Airflow's {{execution_date}}

logger = logging.getLogger(task_id + " " + exec_time)

# grab parameters from config.json
with open('config.json', 'r') as f:
    config_dict = json.load(f)
logger.info(config_dict)

task_dictionary 		= config_dict[task_id]
sql_file		= 'sql/' + task_dictionary['sql_file']
database		= env.upper()
exec_time 	= datetime.strptime(exec_time , "%Y-%m-%dT%H:%M:%S")
task_dictionary['database'] 	= database
task_dictionary['audit_job_id']	= audit_job_id
task_dictionary['bucket']		= config_dict['s3'][env]['bucket']
task_dictionary['logical_dt']	= logical_dt
task_dictionary['effective_timestamp']		= effective_timestamp
task_dictionary['delta_timestamp']	= delta_timestamp
logger.info(task_dictionary)

if __name__ == '__main__':

	# execute snowflake queries
	with SnowflakeConnection() as o:
	    status = o.execute_sql_file(sql_file, task_dictionary)
	logger.info("Status after executing " + sql_file + " is " + str(status))
