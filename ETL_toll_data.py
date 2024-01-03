from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

default_args = {
    'owner' : 'dindonw',
    'start_date':days_ago(0),
    'email':['dindonw@gmail.com'],
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

transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = 'tr "[a-z]" "[A-Z]" < \
    /home/project/airflow/dags/finalassignment/extracted_data.csv \
    > /home/project/airflow/dags/finalassignment/transformed_data.csv',
    dag = dag
)
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
