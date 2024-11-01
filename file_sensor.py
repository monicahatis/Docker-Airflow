from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.exceptions import AirflowSensorTimeout
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
default_args = {
'start_date': datetime(2021, 1, 1)
}
def _done():
    pass
def _partner_a():
    return False
def _partner_b():
    return True
def _failure_callback(context):
    if isinstance(context['exception'], AirflowSensorTimeout):
        print(context)
        print("Sensor timed out")

with DAG('my_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    partner_a = PythonSensor(
        task_id='partner_a',
        poke_interval=120,
        timeout=10,
        mode="reschedule",
        python_callable=_partner_a,
        on_failure_callback=_failure_callback,
        soft_fail=True
    )
    partner_b = PythonSensor(
        task_id='partner_b',
        poke_interval=120,
        timeout=10,
        mode="reschedule",
        python_callable=_partner_b,
        on_failure_callback=_failure_callback,
        soft_fail=True
    )

    file_sensor_task = FileSensor(
        task_id="file_sensor_task",
        poke_interval=30,
        filepath="/usr/local/airflow/Greet.txt",
        timeout=30,
        soft_fail=False
    )

    read_file = BashOperator(
        task_id='read_file',
        bash_command='cat ~/Greet.txt',
        )

    done = PythonOperator(
        task_id="done",
        python_callable=_done,
        trigger_rule='none_failed'
    )
[partner_a, partner_b] >> done
file_sensor_task >> read_file
