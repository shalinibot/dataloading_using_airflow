create or replace table {database}.{tgt_table} as
SELECT distinct song_id, title, artist_id, year, duration,
'{audit_job_id}' as audit_job_id, current_timestamp as audit_create_timestamp
        FROM {database}.staging_songs;