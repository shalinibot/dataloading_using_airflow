insert into {database}.{tgt_table}
select songplay_id,
                start_time, 
                userid, 
                level, 
                song_id, 
                artist_id, 
                sessionid, 
                location, 
                useragent,
                '{audit_job_id}' as audit_job_id, 
                current_timestamp as audit_create_timestamp
from {database}.{stg_table} ;