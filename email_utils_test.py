from airflow.operators.email_operator import EmailOperator
import logging as log
import boto3
import pandas as pd

def email_wrapper(task_name,subject, body, dag, to_email, file_path, **kwargs):
    email = EmailOperator(
        task_id=task_name,
        to=[to_email],
        subject=subject,
        html_content=body,
        mime_charset='utf-8',
        files=file_path,
        dag=dag
    )
    print(kwargs)
    email.execute(kwargs)
    log.info(f"""email operator for subject: '{subject}', ran successfully""")
    
    

def filetoemail(target_path,outputpath,temp_path,task_name,subject,body,dag,to_email):
  print('&&&&&&&&&&&&&&&&&')
  print(temp_path)
  print(to_email)
  print(task_name)
  print(subject)
  print(body)
  oBucket=target_path.split('//')[1].split('/')[0]
  print(oBucket)
  oPrefix=target_path.replace(oBucket+'/','').split('//')[1]
  print(oPrefix)
  print('file movement_triggered')
  s3 = boto3.client('s3')
  s3_resource = boto3.resource('s3')
  resp = s3.list_objects_v2(Bucket=oBucket, Prefix=oPrefix)
  for obj in resp['Contents']:
    if 'part' in obj['Key']:
        print('**************')
        print(obj['Key'])
        df = pd.read_csv('s3://{0}/{1}'.format(oBucket,obj['Key']))
        df.to_csv(outputpath)  
        df.to_csv(temp_path)
        email_wrapper(task_name,subject, body, dag, to_email, ['/tmp/amrit_dpd.csv'])
        
