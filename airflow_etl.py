from datetime import datetime
from airflow import DAG

default_args = {
    'owner' : 'dindonw',
    'start_date':days_ago(0),
    'email':['dummyemail@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries':1,
    'retry_delay':timedelta(minutes = 5)
}

dag = DAG(
     'ETL_toll_data',
    default_args = default_args,
    description = 'apache airflow final assignment',
    schedule_interval = timedelta(days = 1)
)

unzip_data = BashOperator(
    task_id = 'upzip_data',
    bash_command = 'tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag = dag
)

extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d"," -f1-4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > \
    /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag = dag
)

extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'cut -d -f5-7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > \
    /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag = dag
)

extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = 'cut -c59-67 /home/project/airflow/dags/finalassignment/payment-data.txt\
    |tr " " "," > \
    /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
    dag = dag
)

consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = 'paste /home/project/airflow/dags/finalassignment/csv_data.csv \
    /home/project/airflow/dags/finalassignment/tsv_data.csv \
    /home/project/airflow/dags/finalassignment/fixed_width_data.csv \
    > /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag = dag
)