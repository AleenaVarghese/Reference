import pandas as pd 
import numpy as np
import json 
import datetime
from pymongo import MongoClient 
from pandas.io.json import json_normalize
from openpyxl import load_workbook

connection_string = ""
client = MongoClient(connection_string)
db = client['videx-production']
collection = db['loan_transactions']

def get_data(data_category):
    df = pd.DataFrame(collection.find({"_type": "TermLoan::Transaction","transaction_state":data_category},{"_id" : -1,
    "client_name" : 1,
    "security_type" : 1 ,
    "deal_name" : 1 ,
    "facility_limit" : 1 ,
    "tranches" : 1 ,
    "tranches_count" : 1 ,
    "interest" : 1 ,
    "moratorium" : 1 ,
    "tenor_in_months" : 1 ,
    "processing_fee" : 1 ,
    "disbursement_date" : 1 ,
    "interest_repayment_frequency" : 1 ,
    "principal_repayment_frequency" : 1 ,
    "repayment_type" : 1 ,
    "availability_period" : 1 ,
    "_type" : 1 ,
    "rating" : 1 ,
    "protection" : 1 ,
    "security_type" : 1 ,}))
    df = df.where(pd.notnull(df), None)
    
    df["Rate of Interest %"] = [d.get('effective_interest_rate')if d is not None else None for d in df["interest"] ]
    df["Floating/Fixed"] = [d.get('type')if d is not None else None for d in df["interest"] ]
    df["Spread %"] = [d.get('spread')if d is not None else None for d in df["interest"] ]
    df["Interest Reset frequency"] = [d.get('spread_reset_frequency')if d is not None and d.get('type') == 'floating' else None for d in df["interest"] ]
    df["Security Cover Typr"] = ""
    for idx,data in  enumerate(df["protection"]):
        if data is None :
            df.loc[idx, 'Security Cover Typr'] = ""
        elif data.get('security') is not None :
            if data.get('security').get('charge_type') is None :
                df.loc[idx, 'Security Cover Typr'] = ""
            else:
                df.loc[idx, 'Security Cover Typr'] = data.get('security').get('charge_type')
                print("--------",data.get('security').get('charge_type'))
        else:
            df.loc[idx, 'Security Cover Typr'] = ""

    df["Security Cover %"] = ""
    for idx,data in  enumerate(df["protection"]):
        if data is None :
            df.loc[idx, 'Security Cover %'] = ""
        elif data.get('security') is not None :
            if data.get('security').get('security_cover') is None :
                df.loc[idx, 'Security Cover %'] = ""
            else:
                df.loc[idx, 'Security Cover %'] = data.get('security').get('security_cover')
                print("--------",data.get('security').get('security_cover'))
        else:
            df.loc[idx, 'Security Cover %'] = ""

    df["Guarantee(Personal/Corporate)"] = ""
    for idx,data in  enumerate(df["protection"]):
        if data is None :
            df.loc[idx, 'Guarantee(Personal/Corporate)'] = ""
            continue
        df.loc[idx, 'Guarantee(Personal/Corporate)'] = ""
        
        if data.get('corporate_guarantee') is not None: #if corporate_guarantee key is avail
            if data.get('corporate_guarantee') != []:
                df.loc[idx, 'Guarantee(Personal/Corporate)'] = "Corporate Guarantee"

        
        if data.get('promoter_guarantee') is not None: #if corporate_guarantee key is avail
            if data.get('promoter_guarantee') != []:
                if len(df.loc[idx, 'Guarantee(Personal/Corporate)']) == 0:
                    df.loc[idx, 'Guarantee(Personal/Corporate)'] = "Promoter Guarantee"
                else: 
                    df.loc[idx, 'Guarantee(Personal/Corporate)'] +=  ", Promoter Guarantee"
    
        if df.loc[idx, 'Guarantee(Personal/Corporate)'] is None:
            df.loc[idx, 'Guarantee(Personal/Corporate)'] = ""

    df["Disbursement Amount(Crs)"] = ""
    for idx,data in  enumerate(df["tranches"]):
        if data is None :
            df.loc[idx, 'Disbursement Amount(Crs)'] = ""
            continue
        disb_amount = 0
        trunche_count = 0
        for tr in data:
            disb_amount += tr["amount"] if tr["state"] == "disbursed" else 0
            trunche_count += 1 if tr["state"] == "disbursed" else 0
        df.loc[idx, 'Disbursement Amount(Crs)'] = disb_amount if disb_amount !=0 else None
        df.loc[idx, 'Tranche'] = "Tranche" + str(trunche_count) if trunche_count != 0 else None

    df = df.rename(columns={"client_name":"Client Name","security_type":"Secured/ Unsecured","deal_name":"Deal ID","moratorium":"Moratorium","facility_limit":"Sanction Amount(Crs)",
                        "tenor_in_months"	:	"Tenor(Months)","interest_repayment_frequency"	:	"Interest Repayment Frequency","repayment_type"	:	"Repayment Type",
                        "rating"	:	"Rating" ,"processing_fee"	:	"Processing Fee %", "principal_repayment_frequency"	:	"Principal Repayment Frequency",
                        "availability_period"	:	"Avilability Period(Months)" , "disbursement_date"	:	"Disbursement Date" ,"_type"	:	"Typer of Loan(TL/ WCDL)"})

    df1 = df[[ "Client Name",
                "Deal ID",
                "Sanction Amount(Crs)",
                "Disbursement Amount(Crs)",
                "Tranche",
                "Rate of Interest %",
                "Floating/Fixed",
                "Spread %",
                "Interest Reset frequency",
                "Moratorium",
                "Tenor(Months)",
                "Processing Fee %",
                "Disbursement Date",
                "Interest Repayment Frequency",
                "Principal Repayment Frequency",
                "Repayment Type",
                "Security Cover Typr",
                "Security Cover %",
                "Avilability Period(Months)",
                "Typer of Loan(TL/ WCDL)",
                "Rating",
                "Guarantee(Personal/Corporate)",
                "Secured/ Unsecured"
                ]]
    return df1

df_matured = get_data("matured")
df_settled = get_data("settled")
df_finalised = get_data("finalized")

date = str(datetime.datetime.now()).replace(" ","").replace(":","_").replace(".","_")
file_name = "./Loans Report"+str(date)+".xlsx"
with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:    
    df_finalised.to_excel(writer, sheet_name="Finalised",index=False)
    df_settled.to_excel(writer,sheet_name="Settled",index=False)
    df_matured.to_excel(writer,  sheet_name="Matured",index=False)   
     

