#airflow outer file script:
from datetime import timedelta, date, datetime
from airflow import DAG
import pendulum
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from vivriti.airflow.colending_dashboard.python.colending_report_generation import generate_colending_reports
from airflow.contrib.operators.aws_sqs_publish_operator import SQSPublishOperator
from airflow.models import Variable, TaskInstance
from airflow.utils.trigger_rule import TriggerRule
from vivriti.airflow.colending_investor_integration.python.colending_vcpl_slice_interest_computation_report import get_vcpl_slice_interest_computation_report_daily

s3_bucket = 's3://' + Variable.get("de_s3_raw_bucket") + '/'
output_path = 'colending/investor_reports/vcpl/' + '{{ ds }}'
temp_path = Variable.get("tmp_path")



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':  datetime(2022, 1, 1, tzinfo=pendulum.timezone('UTC')),
    'email': ['de-notification@credavenue.com'],
    'email_on_failure': True,
    'email_on_retry': True
}

dag = DAG(
    'vcpl_slice_interest_computation_report',
    default_args=default_args,
    description='Vcpl Slice Interest Computation Report',
    schedule_interval='00 18 L * *',  # Everyday at 11.30 PM in IST
    tags = ['PI']
)

start = DummyOperator(task_id='start',
                      dag=dag)

interest_computation_report = PythonOperator(task_id='vcpl_slice_interest_computation_report_generation',
                                          python_callable=get_vcpl_slice_interest_computation_report_daily,
                                          dag=dag,
                                          op_kwargs={
                                              's3_path': s3_bucket + output_path + '/',
                                              'run_date': '{{ ds }}',
                                              'temp_path': temp_path ,
                                              'dag': dag,
                                              'email_recipients': Variable.get(
                                                  'vcpl_slice_interest_computation_report_email_recipients')},
                                          )

end = DummyOperator(task_id='End',
                    dag=dag)

start >> interest_computation_report >> end
