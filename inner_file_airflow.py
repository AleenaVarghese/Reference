import datetime
import os
from dateutil import parser
from datetime import date, timedelta
import datetime as ds
import pandas as pd
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
from vivriti.utils.email_utils import email_wrapper
from airflow.models import Variable
from vivriti.utils import decryption_utils
import numpy as np
import requests
import json 

ts = str(ds.datetime.now()).replace(' ', '_')

def get_connection_variable(connection_variable):
    print('Establishing the postgres connection')
    airflow_connection = BaseHook.get_connection(connection_variable)
    connection_string = "postgres://" + airflow_connection.login + ":" + airflow_connection.password + "@" + airflow_connection.host + ":" + str(
        airflow_connection.port) + "/" + airflow_connection.schema
    return connection_string

def decrypt_aadhar_reference(aadhar_reference):
    connection = BaseHook.get_connection('aadhar_service')
    if aadhar_reference != '':
        url = f"aadhar_number/requestor_name/de_mfl_truecredit_disb_report"
        headers = {'api-key': f'{connection.password}', 'app-name': 'colending'}
        response = requests.get(url, headers=headers)
        if response.text is None or response.text == '':
            print(f"Response is empty : {response.text}")
        else:
            aadhar_dict = json.loads(response.text)
            if 'aadhar_number' in aadhar_dict.keys():
                return aadhar_dict['aadhar_number']
    return ''


def s3_connect():
    s3 = S3Hook(aws_conn_id='aws_default')
    s3.get_conn()
    return

def get_disb_report_daily(s3_path, temp_path, email_recipients, dag):
    print("START::MFL <> Truecredit Disb report Triggered")
    
    RDS_connection = get_connection_variable('colending_postgres')
    investor_id = json.loads(Variable.get('colending_mfl_truecredit_filter_condition'))['investor_id']
    customer_id = json.loads(Variable.get('colending_mfl_truecredit_filter_condition'))['customer_id']

    #get date from airflow variables
    start_date_var = Variable.get('colending_mfl_truecredit_start_date')
    date_qry = """
        select now() as "End Date";
    """
    print(date_qry)
    date_df = pd.DataFrame()
    date_df = pd.read_sql(date_qry, RDS_connection)
    end_date_var = date_df['End Date'][0]#2023-01-25 12:26:03.692173
    query = """ 
    select * from tabl1
    where loans.status in (5,6,7) and 
    alliances.investor_id = '{0}' --#mfl
    and alliances.customer_id = '{1}'
    and loans.client_disbursed_date between '{2}' and '{3}'
    """.format(investor_id,customer_id,start_date_var,end_date_var)
    print(query)
    df = pd.read_sql(query,con=RDS_connection)
    print(df)
    #decript script
    columns_to_be_decrypted = ["Customer Name","PAN No","Mobile Number","Bank A/c No"]
    integer_columns = ["Mobile Number"]
    #df["bor_aadhar","co_aadhar"]
    df = decryption_utils.decrypt_cyphertext(df, columns_to_be_decrypted, integer_columns)
    #df = decryption_utils.decrypt_cyphertext(df, ["co_aadhar"])
    print(df[["Customer Name","PAN No","Mobile Number"]])
    
    #format phone no
    df['Mobile Number'] = df['Mobile Number'].str.strip('[\'').str.strip('\']')

    # Uppercase
    df["PAN No"] = df["PAN No"].str.upper()
    df["Customer Name"] = df['Customer Name'].str.upper()
    
    # Converting the start date and End Date from UTC to IST for Email Body and file name
    start_date = parser.parse(str(start_date_var))
    start_date = start_date + timedelta(hours=5) + timedelta(minutes =30)
    start_date = datetime.datetime.strftime(start_date, '%Y-%m-%d %H:%M:00')

    end_date = parser.parse(str(end_date_var))
    end_date = end_date + timedelta(hours=5) + timedelta(minutes =30)
    end_date = datetime.datetime.strftime(end_date, '%Y-%m-%d %H:%M:00')

    #create file name
    date_tdy = datetime.datetime.strftime(parser.parse(str(end_date)).date(),'%d%m%y')
    fromtime= datetime.datetime.strftime(parser.parse(str(start_date)),'%H%M%S')
    totime = datetime.datetime.strftime(parser.parse(str(end_date)),'%H%M%S')
    print('start_date = ',start_date," end_date = ",end_date)
    print(date_tdy,'< >',fromtime,'< >',totime)

    #Get 'DetAppID','PLLOSAppNumber' from int DB
    #get unique loan_ids from df 
    requestable_id_list = tuple([str(loan_id) for loan_id in df['loan_id'] if str(loan_id).strip() != ''])
    print("requestable_id_list=",requestable_id_list)
    if len(requestable_id_list) > 0:
        RDS_connection_int = get_connection_variable('colending_postgres_integration')
        int_df1 = pd.read_sql(
            """
            select investor_identifier_maps.value as "PLLOSAppNumber",requestable_id as loan_id from investor_identifier_maps
            inner join investor_requests on
            investor_requests.id = investor_identifier_maps.investor_request_id
            where 
            investor_identifier_maps.key = 'PLLOSAppNumber' and requestable_id in """+str(requestable_id_list).replace(",)",")")
            , RDS_connection_int)
        int_df1['loan_id'] = int_df1['loan_id'].astype('int')
        print("int_df1=",int_df1)

        int_df2 = pd.read_sql(
            """
            select investor_identifier_maps.value as "PLLoanNumber",requestable_id as loan_id from investor_identifier_maps
            inner join investor_requests on
            investor_requests.id = investor_identifier_maps.investor_request_id
            where 
            investor_identifier_maps.key = 'PLLoanNumber' and requestable_id in """+str(requestable_id_list).replace(",)",")")
            , RDS_connection_int)
        int_df2['loan_id'] = int_df2['loan_id'].astype('int')
        print("int_df2=",int_df2)

        final_df = df.merge(int_df1,on='loan_id',how='left')
        final_df = final_df.merge(int_df2,on='loan_id',how='left')

        final_df = final_df.rename(columns={'PLLoanNumber':'Muthoot Loan Number','PLLOSAppNumber':'Muthoot Application Number'})
    else:
        final_df = df 
        final_df['Muthoot Application Number'] = ''
        final_df['Muthoot Loan Number'] = ''
        
    final_df = final_df[['Partner Loan No ','Muthoot Application Number', 'Muthoot Loan Number','Customer Name', 'Mobile Number',
       'PAN No', 'Loan Amount', 'PF without GST', 'PF GST', 'Net Disb',
       'ROI(Anual)', 'Tenure', 'EMI Amount', 'EMI Cycle', 'EMI Start Date',
       'EMI End Date', 'Bank A/c No', 'Bank Name', 'IFSC Code']]
    print("final_df=",final_df)

    s3_connect()
    s3path = s3_path + "MFL_TrueCredit_Disbursement_Report_{0}_{1}_{2}.xlsx".format(date_tdy,fromtime,totime)
    temp_path = temp_path +  "MFL_TrueCredit_Disbursement_Report_{0}_{1}_{2}.xlsx".format(date_tdy,fromtime,totime)
    print(s3path)
    final_df.to_excel(s3path, index=False)
    final_df.to_excel(temp_path, index=False)
    
    print("Report generated - Email triggering")
    email_wrapper('MFL_TrueCredit_Disbursement_Report_Email',
                  'MFL <> TrueCredit Disbursement Report - Cases approved  during {0} to {1}'.format(start_date, end_date),
                  """<h3>MFL TrueCredit Disbursement Report is attached below </h3>""",
                  dag, email_recipients,
                   [temp_path])
    os.remove(temp_path)
    Variable.set("colending_mfl_truecredit_start_date", end_date_var) #update airflow variable
    print("END::MFL <> TrueCredit Disbursement Report sent for the batch time {0} - {1}".format(start_date, end_date))

def get_repayment_report_daily(s3_path, temp_path, email_recipients, dag):
    print("START::MFL <> Truecredit Repayment report Triggered")
    
    RDS_connection = get_connection_variable('colending_postgres')
    investor_id = json.loads(Variable.get('colending_mfl_truecredit_filter_condition'))['investor_id']
    customer_id = json.loads(Variable.get('colending_mfl_truecredit_filter_condition'))['customer_id']

    #get date from airflow variables
    start_date_var = Variable.get('colending_mfl_truecredit_repayment_start_date')
    date_qry = """
        select now() as "End Date";
    """
    print(date_qry)
    date_df = pd.DataFrame()
    date_df = pd.read_sql(date_qry, RDS_connection)
    end_date_var = date_df['End Date'][0]#2023-01-25 12:26:03.692173
    query = """ 
        
    """.format(investor_id,customer_id,start_date_var,end_date_var)
    print(query)
    df = pd.read_sql(query,con=RDS_connection)
    print(df)
    #decript script
    columns_to_be_decrypted = ["Customer Name","Mobile Number"]
    integer_columns = ["Mobile Number"]
    #df["bor_aadhar","co_aadhar"]
    df = decryption_utils.decrypt_cyphertext(df, columns_to_be_decrypted, integer_columns)
    #df = decryption_utils.decrypt_cyphertext(df, ["co_aadhar"])
    print(df[["Customer Name","Mobile Number"]])
    
    #format phone no
    df['Mobile Number'] = df['Mobile Number'].str.strip('[\'').str.strip('\']')

    # Uppercase
    df["Customer Name"] = df['Customer Name'].str.upper()
    
    # Converting the start date and End Date from UTC to IST for Email Body and file name
    start_date = parser.parse(str(start_date_var))
    start_date = start_date + timedelta(hours=5) + timedelta(minutes =30)
    start_date = datetime.datetime.strftime(start_date, '%Y-%m-%d %H:%M:00')

    end_date = parser.parse(str(end_date_var))
    end_date = end_date + timedelta(hours=5) + timedelta(minutes =30)
    end_date = datetime.datetime.strftime(end_date, '%Y-%m-%d %H:%M:00')

    #create file name
    date_tdy = datetime.datetime.strftime(parser.parse(str(end_date)).date(),'%d%m%y')
    fromtime= datetime.datetime.strftime(parser.parse(str(start_date)),'%H%M%S')
    totime = datetime.datetime.strftime(parser.parse(str(end_date)),'%H%M%S')
    print('start_date = ',start_date," end_date = ",end_date)
    print(date_tdy,'< >',fromtime,'< >',totime)

    #Get 'PLLoanNumber','PLLOSAppNumber' from int DB
    #get unique loan_ids from df 
    requestable_id_list = tuple([str(loan_id) for loan_id in df['loan_id'] if str(loan_id).strip() != ''])
    if len(requestable_id_list) > 0:
        RDS_connection_int = get_connection_variable('colending_postgres_integration')
        int_df1 = pd.read_sql(
            """
            select investor_identifier_maps.value as "PLLOSAppNumber",requestable_id as loan_id from investor_identifier_maps
            inner join investor_requests on
            investor_requests.id = investor_identifier_maps.investor_request_id
            where 
            investor_identifier_maps.key = 'PLLOSAppNumber' and requestable_id in """+str(requestable_id_list).replace(",)",")")
            , RDS_connection_int)
        int_df1['loan_id'] = int_df1['loan_id'].astype('int')
        print("int_df1=",int_df1)

        int_df2 = pd.read_sql(
            """
            select investor_identifier_maps.value as "PLLoanNumber",requestable_id as loan_id from investor_identifier_maps
            inner join investor_requests on
            investor_requests.id = investor_identifier_maps.investor_request_id
            where 
            investor_identifier_maps.key = 'PLLoanNumber' and requestable_id in """+str(requestable_id_list).replace(",)",")")
            , RDS_connection_int)
        int_df2['loan_id'] = int_df2['loan_id'].astype('int')
        print("int_df2=",int_df2)

        final_df = df.merge(int_df1,on='loan_id',how='left')
        final_df = final_df.merge(int_df2,on='loan_id',how='left')

        final_df = final_df.rename(columns={'PLLoanNumber':'Muthoot Loan Number','PLLOSAppNumber':'Muthoot Application Number'})
    
    else:
        final_df = df 
        final_df['Muthoot Application Number'] = ''
        final_df['Muthoot Loan Number'] = ''
    final_df = final_df[['Partner Loan No ', 'Muthoot Application Number', 'Muthoot Loan Number','Customer Name', 'Mobile Number',
       'Loan Date', 'Loan Amount', 'EMI Amount', 'Remarks', 'UTR No', 'Date Of Payment' ]]
    print("final_df=",final_df)

    s3_connect()
    s3path = s3_path + "MFL_TrueCredit_Repayment_Report_{0}_{1}_{2}.xlsx".format(date_tdy,fromtime,totime)
    temp_path = temp_path +  "MFL_TrueCredit_Repayment_Report_{0}_{1}_{2}.xlsx".format(date_tdy,fromtime,totime)
    print(s3path)
    final_df.to_excel(s3path, index=False)
    final_df.to_excel(temp_path, index=False)
    
    print("Report generated - Email triggering")
    email_wrapper('MFL_TrueCredit_Repayment_Report_Email',
                  'MFL <> TrueCredit Repayment Report - Cases approved  during {0} to {1}'.format(start_date, end_date),
                  """<h3>MFL TrueCredit Repayment Report is attached below </h3>""",
                  dag, email_recipients,
                   [temp_path])
    os.remove(temp_path)
    Variable.set("colending_mfl_truecredit_repayment_start_date", end_date_var) #update airflow variable
    print("END::MFL <> TrueCredit Repayment Report sent for the batch time {0} - {1}".format(start_date, end_date))
