from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator, LoadFactOperator,LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Rhys Jervis',
    'start_date': datetime(2019, 1, 12),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default':False,
    'depends_on_past':False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)



stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    s3_bucket='udacity-dend',
    s3_key='log_data',
    table='staging_events',
    path = 's3://udacity-dend/log_json_path.json',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",

)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_bucket='udacity-dend',
    s3_key='song_data',
    table='staging_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    

)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id="redshift",
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    sql=SqlQueries.user_table_insert,
    redshift_conn_id="redshift"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    sql=SqlQueries.song_table_insert,
    redshift_conn_id="redshift"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    sql=SqlQueries.artist_table_insert,
    redshift_conn_id="redshift"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    sql=SqlQueries.time_table_insert,
    redshift_conn_id="redshift"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator   >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift 


stage_events_to_redshift  >>  load_songplays_table
stage_songs_to_redshift  >> load_songplays_table

load_songplays_table  >> load_user_dimension_table
load_songplays_table  >> load_song_dimension_table
load_songplays_table >>  load_artist_dimension_table  
load_songplays_table >>  load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
