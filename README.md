# CLOUD DATA WAREHOUSE 
___
### OVERVIEW 
These files include the schema and ETL steps necessary to build a cloud data warehouse for a music streaming service. The database contains songplay log information, as well as user, song, and artist information. The database can be queried to report on business performance or deliver user and product insights.  

The data warehouse is built on AWS using Redshift and S3. The ETL extracts the play logs and song data from S3 buckets and loads it into the staging tables. It then transforms the data and populates the final OLAP tables.  
___
### SCHEMA
The final OLAP database is modeled in a start schema with 5 tables.  
* songplays - **fact table**
    - songplay_id
    - start_time
    - user_id
    - level
    - song_id
    - artist_id
    - session_id
    - location
    - user_agent
* users
    - user_id
    - first_name 
    - last_name 
    - gender
    - level
* songs
    - song_id
    - title
    - artist_id 
    - year 
    - duration
* artists
    - artist_id 
    - name
    - location
    - latitude 
    - longitude
* time
    - start_time 
    - hour
    - day 
    - week 
    - month 
    - year 
    - weekday  

___
### SETUP
To initialize the staging and final tables, first run **create_tables.py** using an existing Redshift cluster. Next, run **etl.py** to perform all of the ETL steps.  
##### You will need a configuration file named ```dwh.cfg``` with the following:  
```
[AWS]
KEY=
SECRET=

[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=

[IAM_ROLE]
ARN=

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
```  
___
### FILES
create_tables.py - this file connects to AWS and initializes the staging tables and final tables in Redshift.
etl.py - this file extracts the data from the S3 bucket, loads it into the staging tables, and transform and loads it into the final tables. 
sql_queries.py - this file contains all of the sql statements used by create_tables.py and etl.py


