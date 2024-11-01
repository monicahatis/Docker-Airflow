from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime



def example_function(**context):
    print("Hello World!")
    context['ti'].xcom_push(key='title', value='Data Consultant')

def another_example_function(**context):
    instance = context.get("ti").xcom_pull(key="title")
    print("Function two")
    return instance


with DAG(
    dag_id='first_dag',
    schedule_interval="@daily",
    default_args={
        "owner":"airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "start_date": datetime(2022,5,8)
    },
    catchup=False
    ) as f:
    
    example_function = PythonOperator(
        task_id="example_function",
        python_callable=example_function,
        provide_context=True,
        op_kwargs={"name":"Oghosa"}
    )

    another_example_function = PythonOperator(
        task_id="another_example_function",
        python_callable=another_example_function,
        provide_context=True
    )

example_function >> another_example_function