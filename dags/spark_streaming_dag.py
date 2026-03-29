from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import socket

# Spark command (run inside spark container)
SPARK_SUBMIT_CMD = "docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.postgresql:postgresql:42.6.0 --conf spark.jars.ivy=/tmp/.ivy /opt/spark-apps/streaming_job.py"

# checking health
def check_spark_job():
    try:
        socket.create_connection(("spark-master", 7077), timeout=5)
        print("Spark cluster reachable")
    except Exception:
        raise Exception("Spark cluster NOT reachable")

# DAG definition
with DAG(
    dag_id="spark_streaming_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Start Spark
    start_spark = BashOperator(
        task_id="start_spark_job",
        bash_command=SPARK_SUBMIT_CMD + " &"
    )

    # Health check
    check_spark = PythonOperator(
        task_id="check_spark_health",
        python_callable=check_spark_job
    )

    # Restart logic
    restart_spark = BashOperator(
        task_id="restart_spark_job",
        bash_command=SPARK_SUBMIT_CMD + " &",
        trigger_rule="one_failed"
    )

    start_spark >> check_spark >> restart_spark