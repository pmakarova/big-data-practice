from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def success_task():
    print("Первая задача выполнена успешно! Сырые данные на месте.")

# падает первые 2 раза и срабатывает на 3-й
def failing_but_retry_task(**kwargs):
    ti = kwargs['ti']
    try_number = ti.try_number  
    
    print(f"Текущая попытка запуска задачи: {try_number}")
    
    if try_number < 3:
        print("Вызываем сбой...")
        raise RuntimeWarning("ClickHouse/Spark временно недоступен. Требуется retry.")
    
    # На 3-ю попытку задача пройдет успешно
    print("Данные обработаны!")

default_args = {
    'owner': 'pvlmakarova',
    'start_date': datetime(2026, 5, 1),
    'retries': 3,                           
    'retry_delay': timedelta(seconds=10),   
}

with DAG(
    dag_id='taxi_big_data_pipeline',
    default_args=default_args,
    schedule_interval=None,                 #
    catchup=False
) as dag:

    task_1 = PythonOperator(
        task_id='check_ingestion_status',
        python_callable=success_task
    )

    task_2 = PythonOperator(
        task_id='run_spark_calculation',
        python_callable=failing_but_retry_task,
        provide_context=True
    )

    task_1 >> task_2