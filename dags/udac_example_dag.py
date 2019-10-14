from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from create_tables import *


default_args = {
    'owner': 'udacity',
    'start_date': datetime.now() - timedelta(seconds=1), 
    'schedule_interval': None,
    'retries':3,
    'retry_delay':timedelta(minutes=5),
    'email_on_retry':True
}


dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          catchup=False,
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# start-date:   'log_data/2018/11/2018-11-01-events.json'
# end-date:     'log_data/2018/11/2018-11-30-events.json'


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="public.staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials_root",
    s3_bucket="s3_bucket",
    s3_key="log_data",
    #s3_key='log_data/2018/11/{}-{}-{}-events.json',
    region = "us-west-2",
    create_table = staging_events_table_create,
    format = "json 's3://udacity-dend/log_json_path.json'",
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="public.staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials_root",
    s3_bucket="s3_bucket",
    s3_key="song_data",
    region = "us-west-2",
    create_table = staging_songs_table_create,
    format = "json 'auto'",
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="public.songplays",
    redshift_conn_id="redshift",
    create_table = songplay_table_create,
    load_table = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="public.users",
    redshift_conn_id="redshift",
    create_table = user_table_create,
    load_table = SqlQueries.user_table_insert,
    truncateInsert = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="public.songs",
    redshift_conn_id="redshift",
    create_table = song_table_create,
    load_table = SqlQueries.song_table_insert,
    truncateInsert = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="public.artists",
    redshift_conn_id="redshift",
    create_table = artist_table_create,
    load_table = SqlQueries.artist_table_insert,
    truncateInsert = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="public.dimTime",
    redshift_conn_id="redshift",
    create_table = time_table_create,
    load_table = SqlQueries.time_table_insert,
    truncateInsert = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    sql_tests=SqlQueries.tests,
    expected_results=SqlQueries.results
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# start_operator >> stage_events_to_redshift
# stage_events_to_redshift >> stage_songs_to_redshift
# stage_songs_to_redshift >> end_operator

# start_operator >> run_quality_checks

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# load_user_dimension_table >> end_operator
# load_song_dimension_table >> end_operator
# load_artist_dimension_table >> end_operator
# load_time_dimension_table >> end_operator

run_quality_checks >> end_operator