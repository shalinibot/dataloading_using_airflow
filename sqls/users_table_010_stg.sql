create or replace table {database}.{tgt_table} as
	SELECT distinct userid, firstname, lastname, gender, level,
'{audit_job_id}' as audit_job_id, current_timestamp as audit_create_timestamp
        FROM {database}.staging_events
        WHERE page='NextSong' ;