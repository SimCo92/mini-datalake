import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
# needed for downloading the data
from kaggle.api.kaggle_api_extended import KaggleApi
from zipfile import ZipFile
import io


default_args = {
            "owner": "Airflow",
            "start_date": airflow.utils.dates.days_ago(1),
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "email": "youremail@host.com",
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        }

# Download the kaggle data from the Kaggle API
def download_data():
    """
    Download the data from the Kaggle API
    :return: writing the data to a file
    """
    api = KaggleApi()
    api.authenticate()
    file = api.datasets_download('yelp-dataset', 'yelp-dataset')
    return file.write('files.zip')


with DAG(dag_id="ingest_yelp_data_from_api", default_args=default_args, catchup=False) as dag:

    # Download the data from the Kaggle API
    download_data = PythonOperator(
            task_id="download_data",
            python_callable=download_data
    )

    # Unzip
    saving_rates = BashOperator(
        task_id="unzip",
        bash_command="""
        unzip files.zip &&
        rm files.zip
            """
    )

    # copy the data to the HDFS
    store_to_hdfs = BashOperator(
        task_id="store_to_hdfs",
        bash_command="""
            hdfs dfs -mkdir -p raw/yelp-dataset && 
            hdfs dfs -put -f $AIRFLOW_HOME/files/yelp-dataset/ raw/yelp-dataset/
            """
    )

    # Running Spark Job to process the data
    yelp_data_processing = SparkSubmitOperator(
        task_id="yelp_data_processing",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/spark1.py",
        verbose=False
    )


with DAG(dag_id="ingest_yelp_from_local", default_args=default_args, catchup=False) as dag:

    # copy the data to the HDFS
    store_to_hdfs = BashOperator(
        task_id="store_to_hdfs",
        bash_command="""
                hdfs dfs -mkdir -p /raw/yelp-dataset && \
                hdfs dfs -put -f $AIRFLOW_HOME/files/yelp-dataset/ /raw/yelp-dataset/
                """
    )

    # Running Spark Job to process the data
    yelp_data_processing = SparkSubmitOperator(
        task_id="yelp_data_processing",
        conn_id="spark_conn",
        application="/opt/airflow/dags/scripts/spark1.py",
        verbose=False
    )