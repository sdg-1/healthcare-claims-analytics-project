from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from utils import download_zip, extract_csv_from_zip
import os

# Constants
FDA_URL = "https://www.fda.gov/media/89850/download/"
OUTPUT_DIR = "/opt/airflow/dags/fda_data"

def download_and_extract_fda_file(**context):
    """Download FDA file and extract its contents."""
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Download the zip file
    zip_content = download_zip(FDA_URL)
    
    # Extract CSV files from the zip
    dataframes = extract_csv_from_zip(zip_content)
    
    # Save each DataFrame to a CSV file
    for i, df in enumerate(dataframes):
        output_path = os.path.join(OUTPUT_DIR, f"fda_data_{i+1}.csv")
        df.to_csv(output_path, index=False)
        print(f"Saved FDA data to: {output_path}")
    
    return output_path

with DAG(
    dag_id="fda_download",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@monthly",  # Adjust as needed
    catchup=False,
    default_args={"retries": 3},
) as dag:

    start = DummyOperator(task_id="start")
    
    download_fda = PythonOperator(
        task_id="download_fda",
        python_callable=download_and_extract_fda_file,
        provide_context=True,
    )
    
    end = DummyOperator(task_id="end")
    
    start >> download_fda >> end