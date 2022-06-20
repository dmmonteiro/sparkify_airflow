from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (
    StageToRedshiftOperator, LoadFactOperator,
    LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
import create_tables

create_table_commands = {
    'staging_events_table': create_tables.staging_events_create_table,
    'staging_songs_table': create_tables.staging_songs_create_table,
    'songplays_table': create_tables.songplays_create_table,
    'artists_table': create_tables.artists_create_table,
    'songs_table': create_tables.songs_create_table,
    'time_table': create_tables.time_create_table,
    'users_table': create_tables.users_create_table
}

dimension_query_table_commands = {
    'users': SqlQueries.users_table_insert,
    'songs': SqlQueries.songs_table_insert,
    'artists': SqlQueries.artists_table_insert,
    'time': SqlQueries.time_table_insert
}

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 6, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_task = DummyOperator(task_id='begin_execution',  dag=dag)

tables_created_task = DummyOperator(task_id='tables_created', dag=dag)

# dynamically create tasks to create tables
for table_name, table_script in create_table_commands.items():
    create_task = PostgresOperator(
        task_id=f'create_{table_name}',
        dag=dag,
        postgres_conn_id='redshift',
        sql=table_script
    )

    start_task >> create_task >> tables_created_task

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    db_conn_id='redshift',
    db_table='staging_events',
    aws_conn_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    db_extra="FORMAT AS JSON \
        's3://udacity-dend/log_json_path.json' IGNOREHEADER 1",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    db_conn_id='redshift_conn',
    db_table='staging_songs',
    aws_conn_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    db_extra="JSON 'auto'",
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=f'INSERT INTO public.songplays({SqlQueries.songplay_table_insert});'
)

tables_loaded_task = DummyOperator(task_id='tables_loaded', dag=dag)

for table_name in ['songs', 'artists', 'users', 'time']:
    load_dimension_table = LoadDimensionOperator(
        task_id=f'load_{table_name}_dim_table',
        dag=dag,
        postgres_conn_id='redsfhit',
        db_table=table_name,
        mode='delete-load',
        sql=dimension_query_table_commands[table_name]
    )

    load_songplays_table >> load_dimension_table >> tables_loaded_task

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    db_conn_id='redshift',
    dq_checks=[
        {
            'sql': 'SELECT COUNT(*) FROM public.songplays WHERE playid IS NULL;',
            'result': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM public.songs WHERE songid IS NULL;',
            'result': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM public.artists WHERE artistid IS NULL;',
            'result': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM public.users WHERE userid IS NULL;',
            'result': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM public.time WHERE start_time IS NULL;',
            'result': 0
        },
        {
            'sql': 'SELECT 1 FROM public.songplays;',
            'result': 1
        },
        {
            'sql': 'SELECT 1 FROM public.songs;',
            'result': 1
        },
        {
            'sql': 'SELECT 1 FROM public.artists;',
            'result': 1
        },
        {
            'sql': 'SELECT 1 FROM public.users;',
            'result': 1
        },
        {
            'sql': 'SELECT 1 FROM public.time;',
            'result': 1
        }
    ]
)

end_task = DummyOperator(task_id='stop_execution',  dag=dag)

tables_created_task >> stage_events_to_redshift >> load_songplays_table
tables_created_task >> stage_songs_to_redshift >> load_songplays_table
tables_loaded_task >> run_quality_checks >> end_task
