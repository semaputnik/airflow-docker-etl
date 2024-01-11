from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow import DAG

spark_master = "spark://spark:7077"
spark_app_name = "Calculate_customer_courier_conversations"
source_data_path = "/usr/local/spark/resources/data/source"
destination_data_path = "/usr/local/spark/resources/data/proccessed"

default_args = {
    'owner': 'Airflow',
}

with DAG('customer_courier_conversations',
          default_args=default_args,
          description='Calculate customer_courier_conversations report',
          start_date=datetime.now(),
          schedule_interval='@hourly'
          ) as dag:

    start = DummyOperator(task_id="start", dag=dag)

    spark_job = SparkSubmitOperator(
        task_id="spark_job",
        application="/usr/local/spark/app/application.py",
        name=spark_app_name,
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master": spark_master},
        application_args=[source_data_path, destination_data_path],
        dag=dag
    )

    end = DummyOperator(task_id="end", dag=dag)

    start >> spark_job >> end
