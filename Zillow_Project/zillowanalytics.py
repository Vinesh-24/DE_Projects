from airflow import DAG
from datetime import timedelta, datetime
import json
import requests
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator



default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2025,2,11),
    'email' : ['vvinesh1391@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 2,
    'retry_delay' : timedelta(seconds = 15)
}

# Load the JSON config file

with open('/home/ubuntu/airflow/config_api.json', 'r') as config_file:
        api_host_key = json.load(config_file)

now = datetime.now()
dt_now_string = now.strftime("%d%m%Y%H%M%S")

# Define the S3 bucket
s3_bucket = 'zillow-project-transformed-bucket'

# Python callable functions
# To extract data from API

def extract_zillow_data(**kwargs):
    url = kwargs['url']
    headers = kwargs['headers']
    querystring = kwargs['querystring']
    dt_string = kwargs['date_string']
    
    # return headers
    response = requests.get(url, headers=headers, params=querystring)
    response_data = response.json()

    # Specify the output file path
    output_file_path = f"/home/ubuntu/response_data_{dt_string}.json"
    file_str = f'response_data_{dt_string}.csv'

    # Write the response to the file
    with open(output_file_path, "w") as output_file:
           json.dump(response_data, output_file, indent=4)
    output_list = [output_file_path, file_str]
    return output_list



# DAG Flow

with DAG('zillow_analytics_dag',
        default_args = default_args,
        schedule = '@daily',
        catchup = False) as dag:

        # To extract data from the API

        extract_zillow_dat_var = PythonOperator(
            task_id = 'tsk_extract_zillow_data_var',
            python_callable = extract_zillow_data,
            op_kwargs = {'url' : 'https://zillow56.p.rapidapi.com/search',
                         'querystring' : {"location":"houston, tx"},
                         'headers' : api_host_key,
                         'date_string' : dt_now_string
                         }
        )

        # To load data ito S3 Bucket

        load_to_s3 = BashOperator(
            task_id = 'tsk_load_to_s3',
            # Since we have a list for output_list we consider the first one which is output_file_path
            bash_command = 'aws s3 mv {{ti.xcom_pull("tsk_extract_zillow_data_var")[0]}} s3://zillow-project-bucket/'     
        )

        # S3 Key sensor to check if the data is present or not

        is_file_in_s3_available = S3KeySensor(
            task_id = 'tsk_is_file_in_s3_available',
            bucket_key = '{{ti.xcom_pull("tsk_extract_zillow_data_var")[1]}}',
            bucket_name = s3_bucket,
            aws_conn_id = 'aws_s3_conn',
            wildcard_match = False, # To match the file names. False -> exact match. True -> like match
            timeout = 60,
            poke_interval = 5, # Time interval between S3 checks
        )

        # Transfer the data from S3 to redshift

        transfer_s3_to_redshift = S3ToRedshiftOperator(
            task_id = 'tsk_transfer_s3_to_redshift',
            aws_conn_id='aws_s3_conn',
            redshift_conn_id= 'conn_id_redshift',
            s3_bucket=s3_bucket,
            s3_key='{{ti.xcom_pull("tsk_extract_zillow_data_var")[1]}}',
            method='REPLACE',
            schema='PUBLIC',
            table="zillowdata",
            copy_options=["csv IGNOREHEADER 1"]
        )


# Order of executing the tasks in DAG

extract_zillow_dat_var >> load_to_s3 >> is_file_in_s3_available >> transfer_s3_to_redshift
