from typing import final
import pandas as pd 
import numpy as np
import json 
import datetime
from pymongo import MongoClient 
from pandas.io.json import json_normalize
from openpyxl import load_workbook
import dateutil.parser


connection_string = ""
client = MongoClient(connection_string)

def get_loan_data():
    db = client['videx-production']
    collection = db['loan_transactions']
    loan_df = pd.DataFrame(collection.find({'facility_limit':{'$lt':1500000000},'created_at':{'$gte':dateutil.parser.parse('2021-01-01T00:00:00.000Z')}},{"facility_limit": 1,
            "client_name" : 1,
            "deal_name" : 1, 
            "created_at" : 1,
            "facility_limit" : 1,
            "tenor" : 1,
            "interest" : 1,
            "loan_type" : 1,
            #"customer_id" : 1
            }))
    loan_df = loan_df.where(pd.notnull(loan_df), None)

    loan_df = loan_df.rename(columns={"facility_limit" : "Loan Amount",
            "tenor" : "Expected Tenor ",
            "loan_type" : "Loan Type",
            "client_name" : "Borrower Name" ,
            "deal_name" : "Deal Name" ,
            "created_at" : "Created Date",
            "interest" : "Interest"
        })
    customer_id_list = [x for x in loan_df['customer_id']]
    loan_df_final = loan_df[["Deal Name" ,
    "Borrower Name" ,
    "Created Date",
    "Loan Amount",
    "Expected Tenor ",
    "Loan Type",
    "Interest" ,
    #"customer_id"
    ]]
    return loan_df_final,customer_id_list

loan_df,customer_id_list = get_loan_data()

#merged_df = pd.merge(loan_df,collatral_df, on = "customer_id" ,how ="left")
loan_df.to_excel("./GTM1 Borrowers Report_v1.xlsx", index=False)

#reference

# date = str(datetime.datetime.now()).replace(" ","").replace(":","_").replace(".","_")
# file_name = "./Loans Report"+str(date)+".xlsx"
# with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:    
#     df_finalised.to_excel(writer, sheet_name="Finalised",index=False)
#     df_settled.to_excel(writer,sheet_name="Settled",index=False)
#     df_matured.to_excel(writer,  sheet_name="Matured",index=False)  
