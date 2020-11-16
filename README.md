Purpose

Developing a data pipeline for analysing songs which are played by many users depending on their popularity. We use airflow to automate the entire workflow of this project and also monitor the enitre pipeline.

This project repo contains: 
<br/>
dags - containing the  Airflow dag code<br/>
a file called tables.sql - table defination for all the parent tables involved in this projects
src folder - containing all the src code involved in running the entire project
sqls folder - containing all the sqls which needs to be automated
a file called config.json - used as a parameter file which will be read for all the variables involved in the scripts.
