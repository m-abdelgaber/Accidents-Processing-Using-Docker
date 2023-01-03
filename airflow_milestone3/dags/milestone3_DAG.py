from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd
import numpy as np
# For Label Encoding
import dash
import dash_core_components as dcc
import dash_html_components as html
from sqlalchemy import create_engine

# dataset = '1990_Accidents_UK.csv'
from milestone_processing import milestone1, milestone2
def milestone1_processing(uncleaned_data_path, cleaned_data_output_path, lookup_table_output_path):
    milestone1(uncleaned_data_path, cleaned_data_output_path, lookup_table_output_path)
    print('milestone 1 processing done')
    
    
def milestone2_processing(cleaned_data_path, additional_data_path, integrated_data_output_path ,lookup_table_output_path):
    milestone2(cleaned_data_path, additional_data_path, integrated_data_output_path ,lookup_table_output_path)
    print('milestone 2 processing done')



def create_dashboard(filename):
    app = dash.Dash()
    app.layout = html.Div(
    children=[
        html.H1(children="UK accidents in year 1990",),
        html.P(
            children="Age vs Survived Titanic dataset",
            style={"textAlign": "center"},
        ),
        # dcc.Graph(
        #     figure={
        #         "data": [
        #             {
        #                 "x": df["Age"],
        #                 "y": df["Survived"],
        #                 "type": "lines",
        #             },
        #         ],
        #         "layout": {"title": "Age vs Survived"},
        #     },
        # )
    ]
)
    app.run_server(host='0.0.0.0')
    print('dashboard is successful and running on port 8000')

def load_to_postgres(dataset, lookup_table): 
    df_dataset = pd.read_csv(dataset)
    df_lookup_table = pd.read_csv(lookup_table)
    engine = create_engine('postgresql://root:root@pgdatabase:5432/milestone3')
    if(engine.connect()):
        print('connected succesfully')
    else:
        print('failed to connect')
    try:
        df_dataset.to_sql(name = 'UK_Accidents_1990',con = engine,if_exists='replace')
    except ValueError:
        print('dataset table already exists')
    try:
        df_lookup_table.to_sql(name = 'lookup_table',con = engine,if_exists='replace')
    except ValueError:
        print('lookup table already exists')

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'start_date': days_ago(2),
    "retries": 1,
}

dag = DAG(
    'milestone3_pipeline',
    default_args=default_args,
    description='milestone3 pipeline',
)
with DAG(
    dag_id = 'milestone3_pipeline',
    schedule_interval = '@once',
    default_args = default_args,
    tags = ['titanic-pipeline'],
)as dag:
    milestone1_processing_task= PythonOperator(
        task_id = 'milestone1_processing',
        python_callable = milestone1_processing,
        op_kwargs={
            "uncleaned_data_path": 'data/1990_Accidents_UK.csv',
            "cleaned_data_output_path": 'data/UK_Accidents_1990.csv',
            "lookup_table_output_path": 'data/lookup_table.csv'
        },
    )
    milestone2_processing_task= PythonOperator(
        task_id = 'milestone2_processing',
        python_callable = milestone2_processing,
        op_kwargs={
            "cleaned_data_path": 'data/UK_Accidents_1990.csv',
            "additional_data_path": 'data/pre_integration.csv',
            "integrated_data_output_path": 'data/UK_Accidents_1990.csv',
            "lookup_table_output_path": 'data/lookup_table.csv'
        },
    )
    load_to_postgres_task=PythonOperator(
        task_id = 'load_to_postgres',
        python_callable = load_to_postgres,
        op_kwargs={
            "dataset": "data/UK_Accidents_1990.csv",
            "lookup_table": "data/lookup_table.csv"
        },
    )
    create_dashboard_task= PythonOperator(
        task_id = 'create_dashboard_task',
        python_callable = create_dashboard,
        op_kwargs={
            "filename": "data/UK_Accidents_1990.csv"
        },
    )
    


    milestone1_processing_task >> milestone2_processing_task >> load_to_postgres_task>> create_dashboard_task

    
    



