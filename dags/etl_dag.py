from airflow import DAG
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from airflow.operators.python import PythonOperator
import pandas as pd
import os

# PostgreSQL database connection
DB_URL = "postgresql://airflow:airflow@postgres:5432/airflow"

# output folder paths
WAREHOUSE_PATH = "/opt/airflow/dags/data_warehouse"
REPORTS_PATH = "/opt/airflow/dags/reports"

def etl_to_parquet(**context):
    engine = create_engine(DB_URL)

    # getting the time window for this DAG run
    window_start = context["data_interval_start"]
    window_end = context["data_interval_end"]

    # fetching only the transactions within this 6 hour window
    query = """
        SELECT * FROM transactions
        WHERE timestamp >= %(start)s AND timestamp < %(end)s
    """
    df = pd.read_sql(query, engine, params={"start": window_start, "end": window_end})

    if df.empty:
        print(f"No transactions found between {window_start} and {window_end}")
        return

    validated_df = df[df["fraud_flag"] == "NORMAL"]

    os.makedirs(WAREHOUSE_PATH, exist_ok=True)

    filename = f"validated_transactions_{window_start.strftime('%Y%m%d_%H%M')}.parquet"
    output_path = f"{WAREHOUSE_PATH}/{filename}"

    validated_df.to_parquet(output_path, index=False)

    # pushing stats to XCom for reconciliation task to use
    context["ti"].xcom_push(key="total_ingress", value=len(df))
    context["ti"].xcom_push(key="validated_amount", value=len(validated_df))
    context["ti"].xcom_push(key="window_start", value=str(window_start))
    context["ti"].xcom_push(key="window_end", value=str(window_end))

    # pushing fraud breakdown by merchant category
    fraud_df = df[df["fraud_flag"] != "NORMAL"]
    fraud_by_category = (
        fraud_df.groupby("merchant_category")["fraud_flag"]
        .count()
        .reset_index()
        .rename(columns={"fraud_flag": "fraud_attempts"})
    )
    context["ti"].xcom_push(key="fraud_by_category", value=fraud_by_category.to_dict(orient="records"))

    print(f"Saved {len(validated_df)} validated transactions to {output_path}")


def reconciliation_report(**context):
    ti = context["ti"]

    # pulling stats from etl task via XCom
    total_ingress = ti.xcom_pull(task_ids="etl_to_parquet", key="total_ingress")
    validated_amount = ti.xcom_pull(task_ids="etl_to_parquet", key="validated_amount")
    window_start = ti.xcom_pull(task_ids="etl_to_parquet", key="window_start")
    window_end = ti.xcom_pull(task_ids="etl_to_parquet", key="window_end")
    fraud_by_category = ti.xcom_pull(task_ids="etl_to_parquet", key="fraud_by_category")

    if total_ingress is None:
        print("No data from ETL task, skipping report generation")
        return

    fraud_count = total_ingress - validated_amount

    os.makedirs(REPORTS_PATH, exist_ok=True)

    # reconciliation summary
    summary = pd.DataFrame([{
        "window_start": window_start,
        "window_end": window_end,
        "total_ingress_count": total_ingress,
        "validated_count": validated_amount,
        "fraud_count": fraud_count,
        "validated_percentage": round((validated_amount / total_ingress) * 100, 2) if total_ingress > 0 else 0,
        "fraud_percentage": round((fraud_count / total_ingress) * 100, 2) if total_ingress > 0 else 0,
        "report_generated_at": datetime.now()
    }])

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    summary_path = f"{REPORTS_PATH}/reconciliation_report_{timestamp}.csv"
    summary.to_csv(summary_path, index=False)

    # fraud attempts by merchant category
    if fraud_by_category:
        fraud_category_df = pd.DataFrame(fraud_by_category)
        fraud_category_df["window_start"] = window_start
        fraud_category_df["window_end"] = window_end
        fraud_category_path = f"{REPORTS_PATH}/fraud_by_category_{timestamp}.csv"
        fraud_category_df.to_csv(fraud_category_path, index=False)
        print(f"Fraud by category report saved to {fraud_category_path}")
    else:
        print("No fraud attempts detected in this window")

    print(f"Reconciliation report saved to {summary_path}")
    print(f"Window: {window_start} → {window_end}")
    print(f"Total ingress: {total_ingress} | Validated: {validated_amount} | Fraud: {fraud_count}")


# ETL DAG definition (automatically triggered DAG)
with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 */6 * * *",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:

    etl_task = PythonOperator(
        task_id="etl_to_parquet",
        python_callable=etl_to_parquet,
        provide_context=True
    )

    reconciliation_task = PythonOperator(
        task_id="generate_reconciliation_report",
        python_callable=reconciliation_report,
        provide_context=True
    )

    etl_task >> reconciliation_task