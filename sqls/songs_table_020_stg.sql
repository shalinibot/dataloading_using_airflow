insert into {database}.{tgt_table}
select song_id, title, artist_id, year, duration,
                '{audit_job_id}' as audit_job_id, 
                current_timestamp as audit_create_timestamp
from {database}.{stg_table} ;