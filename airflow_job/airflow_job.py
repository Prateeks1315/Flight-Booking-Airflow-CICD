from datetime import datetime, timedelta
import uuid
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 14),
}

# Define DAG
with DAG(
    dag_id="flight_booking_dataproc_bq_dag",
    default_args=default_args,
    schedule_interval=None,  # Manual or event-based
    catchup=False,
) as dag:

    # Fetch Airflow Variables
    env = Variable.get("env", default_var="dev")
    gcs_bucket = Variable.get("gcs_bucket", default_var="airflow-projetcs-gds")
    bq_project = Variable.get("bq_project", default_var="vigilant-axis-460418-c4")
    bq_dataset = Variable.get("bq_dataset", default_var=f"flight_data_{env}")
    tables = Variable.get("tables", deserialize_json=True)

    transformed_table = tables["transformed_table"]
    route_insights_table = tables["route_insights_table"]
    origin_insights_table = tables["origin_insights_table"]

    # Unique Batch ID
    batch_id = f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"

    # Task 1: Wait for the input file to arrive in GCS
    file_sensor = GCSObjectExistenceSensor(
        task_id="check_file_arrival",
        bucket=gcs_bucket,
        object=f"airflow-project-1/source-{env}/flight_booking.csv",
        google_cloud_conn_id="google_cloud_default",
        timeout=300,
        poke_interval=30,
        mode="poke",
    )

    # Task 2: Run PySpark job on Dataproc Serverless
    pyspark_task = DataprocCreateBatchOperator(
        task_id="run_spark_job_on_dataproc_serverless",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{gcs_bucket}/us-central1-airflow-test-e7feae52-bucket/spark-job/spark_transformation_job.py",
                "args": [
                    f"--env={env}",
                    f"--bq_project={bq_project}",
                    f"--bq_dataset={bq_dataset}",
                    f"--transformed_table={transformed_table}",
                    f"--route_insights_table={route_insights_table}",
                    f"--origin_insights_table={origin_insights_table}",
                ],
            },
            "runtime_config": {
                "version": "2.2",
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "1093218387397-compute@developer.gserviceaccount.com",
                    "network_uri": "projects/vigilant-axis-460418-c4/global/networks/default",
                    "subnetwork_uri": "projects/vigilant-axis-460418-c4/regions/us-central1/subnetworks/default",
                }
            },
        },
        batch_id=batch_id,
        project_id="vigilant-axis-460418-c4",  # Dataproc GCP Project ID
        region="us-central1",
        gcp_conn_id="google_cloud_default",
    )

    # Set Task Dependencies
    file_sensor >> pyspark_task
