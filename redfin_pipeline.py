from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import boto3

s3_client = boto3.client('s3')

target_bucket_name = 'redfin-pipeline-transformed-bucket'

url_by_city = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz'

def extract_data(**kwargs):
    url = kwargs['url']
    df = pd.read_csv(url, compression='gzip', sep='\t')
    now= datetime.now()
    date_now_string= now.strftime("%d%m%Y%H%M%S")
    file_str= 'redfin_data_' + date_now_string
    df.to_csv(f"{file_str}.csv", index=False)
    output_file_path = f"/home/ubuntu/{file_str}.csv"
    output_list= [output_file_path, file_str]
    return output_list


def transform_data(task_instance):
    data = task_instance.xcom_pull(task_ids = "task_extract_redfin_data")[0]
    object_key = task_instance.xcom_pull(task_ids = "task_extract_redfin_data")[1]
    df = pd.read_csv(data)

    # remove commas from the city column
    df['CITY'] = df['CITY'].str.replace(',', '')

    #define the columns that I want in the cleanedup data (I only chose columns that are necessary for the analysis I want to perform at the end of the project)
    columns_to_keep = [
    'PERIOD_BEGIN', 'PERIOD_END', 'CITY', 'STATE', 'REGION', 'PROPERTY_TYPE', 'IS_SEASONALLY_ADJUSTED',
    'MEDIAN_SALE_PRICE', 'MEDIAN_SALE_PRICE_MOM', 'MEDIAN_SALE_PRICE_YOY',
    'MEDIAN_LIST_PRICE',
    'MEDIAN_PPSF',
    'HOMES_SOLD', 'HOMES_SOLD_MOM', 'HOMES_SOLD_YOY',
    'NEW_LISTINGS',
    'INVENTORY', 'MONTHS_OF_SUPPLY',
    'MEDIAN_DOM',
    'AVG_SALE_TO_LIST',
    'SOLD_ABOVE_LIST'
    ]
    df = df[columns_to_keep]

    # now, drop all missing values 
    df = df.dropna()

    #change the dataype of the date columns in the table 
    df['PERIOD_BEGIN'] = pd.to_datetime(df['PERIOD_BEGIN'])
    df['PERIOD_END'] = pd.to_datetime(df['PERIOD_END'])

    # Now, lets print the total number of rows and columns. This would be on the logs in airflow. 
    rows = len(df)
    columns = len(df.columns)

    print(f'Total number of rows: {rows}')
    print(f'Total number of columns: {columns}')

    #now, let us conver the dataframe to a csv file. This csv file is going to be stored on our s3 bucket
    csv_data = df.to_csv(index= False)

    #finally, its time to upload the csv file to our s3 bucket
    object_key = f"{object_key}.csv"
    s3_client.put_object(Bucket = target_bucket_name, Key= object_key, Body=csv_data)




default_args= {
    'owner': 'Chiamaka',
    'depends_on_past': False,
    'start_date': datetime(2025,5,9),
    'email': ['okpeamaka8@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
}

with DAG('redfin_pipeline_dag',
        default_args = default_args,
        #schedule_interval = '@weekly',
        catchup = False) as dag:

        extract_redfin_data = PythonOperator(
        task_id = 'task_extract_redfin_data',
        python_callable= extract_data,
        op_kwargs= {'url': url_by_city}
        )

        transform_redfin_data = PythonOperator(
        task_id = 'task_transform_redfin_data',
        python_callable = transform_data
        )

        load_raw_to_s3 = BashOperator(
        task_id = 'task_load_raw_to_s3',
        bash_command = 'aws s3 mv {{ti.xcom_pull("task_extract_redfin_data")[0]}} s3://redfin-pipeline-raw-data'
        )
        
        extract_redfin_data >> transform_redfin_data >> load_raw_to_s3