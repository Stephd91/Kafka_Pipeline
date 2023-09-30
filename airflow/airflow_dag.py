from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# https://stackoverflow.com/questions/20309456/how-do-i-call-a-function-from-another-py-file
from producer import producer_somedata, producer_footdata
from consumer import consumer

# 1) Define the DAG arguments :
default_args = {
    "owner": "Stephd91",
    "start_date": days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

# 2) Define the DAG
dag = DAG(
    "kafka-workflow",
    default_args=default_args,
    description="Start workflow with Kafka, Producer and Consumer APIs",
    schedule_interval=timedelta(minutes=1),
)

# 3) Define the tasks
"""# Task 1 : start kafka zookeeper (TO DO : call kafka_admin.py instead)
start_kafka_zookeeper = BashOperator(
    task_id="kafka-zookeeper",
    bash_command="python3 kafka_admin.py",
    dag=dag,
)
"""

# Task 3.1 : start somedata producer
start_somedata_producer = PythonOperator(
    task_id="producer-somedata",
    python_callable=producer_somedata,
    dag=dag,
)

# Task 3.2 : start footdata producer
start_footdata_producer = PythonOperator(
    task_id="producer-footdata",
    python_callable=producer_footdata,
    dag=dag,
)

# Task 4 : start consumer
start_consumer = PythonOperator(task_id="consumer", python_callable=consumer, dag=dag)


[start_somedata_producer, start_footdata_producer] >> start_consumer
