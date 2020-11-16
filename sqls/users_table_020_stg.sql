insert into {database}.{tgt_table}
select userid, firstname, lastname, gender, level,
                '{audit_job_id}' as audit_job_id, 
                current_timestamp as audit_create_timestamp
from {database}.{stg_table} ;