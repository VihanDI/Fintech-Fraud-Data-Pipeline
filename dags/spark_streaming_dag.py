from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import socket

SPARK_SUBMIT_CMD = (
    "docker exec -d spark-master /bin/bash -c "
    "'/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--driver-memory 1g "
    "--executor-memory 1g "
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.postgresql:postgresql:42.6.0 "
    "--conf spark.jars.ivy=/tmp/.ivy "
    "/opt/spark-apps/streaming_job.py "
    "> /tmp/spark-job.log 2>&1'"
)

KILL_CMD = "docker exec spark-master pkill -f streaming_job.py || true"

def check_spark_cluster():
    # checking if spark master port is reachable
    try:
        socket.create_connection(("spark-master", 7077), timeout=5)
        print("Spark master reachable on port 7077")
    except Exception:
        raise Exception("Spark master NOT reachable on port 7077")


def check_spark_job_running():
    import subprocess

    # checking if streaming_job.py process is running inside spark-master container
    result = subprocess.run(
        ["docker", "exec", "spark-master", "pgrep", "-f", "streaming_job.py"],
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        print(f"Streaming job is running with PID: {result.stdout.strip()}")
    else:
        # checking log for errors
        log_result = subprocess.run(
            ["docker", "exec", "spark-master", "tail", "-20", "/tmp/spark-job.log"],
            capture_output=True,
            text=True
        )
        print("Streaming job is NOT running. Last log lines:")
        print(log_result.stdout)
        raise Exception("Streaming job is not running")


# Streaming DAG definition (manually triggered DAG)
with DAG(
    dag_id="spark_streaming_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # checking spark master is up before submitting
    check_cluster = PythonOperator(
        task_id="check_spark_cluster",
        python_callable=check_spark_cluster
    )

    # submitting the streaming job
    start_spark = BashOperator(
        task_id="start_spark_job",
        bash_command=SPARK_SUBMIT_CMD
    )

    # waiting for job to initialize then verify it's running
    wait_and_verify = PythonOperator(
        task_id="verify_job_running",
        python_callable=check_spark_job_running
    )

    # restarting job if verify step fails
    restart_spark = BashOperator(
        task_id="restart_spark_job",
        bash_command=f"{KILL_CMD} && sleep 5 && {SPARK_SUBMIT_CMD}",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    check_cluster >> start_spark >> wait_and_verify >> restart_spark