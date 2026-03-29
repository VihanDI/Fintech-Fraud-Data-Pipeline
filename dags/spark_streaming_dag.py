from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

# spark command for starting the streaming job
SPARK_SUBMIT_CMD = "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.postgresql:postgresql:42.6.0 --conf spark.jars.ivy=/tmp/.ivy /opt/spark-apps/streaming_job.py"

# streaming job running status checker function
def check_spark_job():
    try:
        output = subprocess.check_output("ps aux | grep streaming_job.py", shell=True).decode()
        
        # removing grep process itself
        lines = [line for line in output.split("\n") if "grep" not in line]

        if len(lines) == 0:
            raise Exception("Spark streaming job NOT running!")

        print("Spark job is running")

    except Exception as e:
        raise Exception(f"Health check failed: {str(e)}")

# DAG definition
with DAG(
    dag_id="spark_streaming_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    # starting Spark streaming job
    start_spark = BashOperator(
        task_id="start_spark_job",
        bash_command=SPARK_SUBMIT_CMD
    )

    # checking health
    check_spark = PythonOperator(
        task_id="check_spark_health",
        python_callable=check_spark_job
    )

    # restarting logic
    restart_spark = BashOperator(
        task_id="restart_spark_job",
        bash_command=SPARK_SUBMIT_CMD,
        trigger_rule="one_failed"
    )

    # DAG flow
    start_spark >> check_spark >> restart_spark