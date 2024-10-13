from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import json
from airflow.hooks.postgres_hook import PostgresHook
import uuid

postgres_hook = PostgresHook(postgres_conn_id="my_prod_db")

def seed_table(json_path, table_name, generate_id=False, column_id_name=None):
    assert (generate_id==False and column_id_name is None) or (generate_id==True and column_id_name is not None)

    # Fetch data from json file
    with open(json_path) as json_file:
        data = json.load(json_file)
    if not data or (len(data)==0):
        raise ValueError("The JSON data is empty")
    
    #Target columns
    columns = list(data[0].keys())

    if generate_id:
        columns.append(column_id_name)
    
    #Extract values to insert
    values = []
    for item in data:
        row_values = tuple( str(uuid.uuid4()) if col==column_id_name else item[col] for col in columns)
        values.append(row_values)        
    
    postgres_hook.insert_rows(
        table=table_name,
        rows=values,
        target_fields=columns,
        commit_every=1000,
    )

default_args = {
}

# Instantiate the DAG
with DAG(
    'seed_base_tables_dag', 
    description="Creates and seeds Category and Products tables.",
    default_args=default_args,
    schedule_interval='@once',
    start_date=datetime(2024, 10, 1),
    catchup=False,
    is_paused_upon_creation=False,
) as dag :

    create_category_table = PostgresOperator(
        task_id='create_category_table',
        postgres_conn_id='my_prod_db',
        sql='''
        DROP TABLE CATEGORY;
        CREATE TABLE CATEGORY (
            category_id INT PRIMARY KEY,
            category_name VARCHAR(255) NOT NULL
        );
        '''
    )

    seed_category_table = PythonOperator(
        task_id='seed_category_table',
        python_callable=seed_table,
        op_kwargs={
            "json_path":"/opt/airflow/data/categories.json",
            "table_name": "category"
        }
    )

    create_product_table = PostgresOperator(
        task_id='create_product_table',
        postgres_conn_id='my_prod_db',
        sql='''
        DROP TABLE PRODUCT;
        CREATE TABLE PRODUCT (
            product_id VARCHAR(255) PRIMARY KEY,
            product_name VARCHAR(255) NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            category_id INT NOT NULL,
            MinStockThreshold INT NOT NULL
        );
        '''
    )

    seed_product_table = PythonOperator(
        task_id='seed_product_table',
        python_callable=seed_table,
        op_kwargs={
            "json_path":"/opt/airflow/data/products.json",
            "table_name": "product",
            "generate_id": True, 
            "column_id_name": "product_id"
        }
    )

    create_category_table >> seed_category_table >> create_product_table >> seed_product_table