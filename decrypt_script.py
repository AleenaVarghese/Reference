#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#from airflow.configuration import conf
#from airflow.models import Variable
# from vivriti.utils.email_utils import email_wrapper
import base64
import boto3
import pandas as pd
import struct
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from base64 import b64decode

# error_email_recipients = Variable.get("decrypt_error_email")

# kms_client = boto3.client('kms', region_name='ap-south-1')
# encrypted_master_key = conf.get('colend', 'encryptionkey')
# hex_encrypted_master_key = bytes.fromhex(encrypted_master_key)
# decrypted_master_key = kms_client.decrypt(CiphertextBlob=hex_encrypted_master_key)
master_key = '5a3a7e812f6cda3b2c819c06472851f010688ad06e862c825b6fa25f80e7f047'


def decryption(ciphertext):
    try:
        ciphertext = b64decode(ciphertext)
        print("1",ciphertext)
        key = bytes.fromhex(master_key)
        print("2")
        aesgcm = AESGCM(key)
        print("3")
        decrypted_text = aesgcm.decrypt(ciphertext[:12], ciphertext[12:], b'')
        print("4",decrypted_text)
        return decrypted_text
    except Exception as e:
        print("#############",e)

def decrypt_cyphertext1(df, columns_to_be_decrypted, integer_columns=[]):
    row_list = []
    original_column_order_list = list(df.columns)
    is_decrypt_error = False
    print("Decrypting Columns -> {}".format(str(columns_to_be_decrypted)))
    for row_ind, row_val in df.iterrows():

        col_list, error_list = [], []
        for col_val in columns_to_be_decrypted:
            # Iterate each row and each decrypting column in dataframe and decrypting only non-null values
            val = row_val[col_val]
            if type(val) == list:
                lst = []
                for each_val in val:
                    if each_val not in ['nan', 'Nan', 'NaN', ''] and isinstance(each_val, float) is False and each_val is not None:
                        try:
                            decrypted_text = decryption(each_val)
                            lst.append(decrypted_text.decode('utf-8'))
                        except Exception as error:
                            is_decrypt_error = True
                            err = str(error)
                            print("-------",err )
                            print("Decryption Error Occured in {} -> {}".format(col_val, each_val))
                            error_list.append("{} -> {}".format(col_val, each_val))
                            lst.append(None)
                    else:
                        lst.append(each_val)
                decrypted_val = str(lst)
            elif val not in ['nan', 'Nan', 'NaN', ''] and isinstance(val, float) is False and val is not None:
                try:
                    decrypted_val = decryption(val)
                    if col_val in integer_columns:
                        decrypted_val = struct.unpack('>q', decrypted_val)
                        decrypted_val = decrypted_val[0]
                    else:
                        decrypted_val = decrypted_val.decode('utf-8')
                except Exception as error:
                    is_decrypt_error = True
                    err = str(error)
                    print("Decryption Error Occured in {} -> {}".format(col_val, val))
                    error_list.append("{} -> {}".format(col_val, val))
                    decrypted_val = None
            else:
                decrypted_val = val

            col_list.append(decrypted_val)
            
#             col_list = [item.replace('\n', '') if "\n" in item else item for item in col_list]
#             try:
                
#                 col_list = [item.replace('\n', '') for item in col_list if ] 
#             except Exception as error:
#                 print("None object")
        row_list.append(col_list)
        
        

#         [[float(y) for y in x] for x in l]



#     if is_decrypt_error:
#         print("Sending Decryption Error Email")
#         email_wrapper('Decryption_Error_Email',
#                       'Decryption Error',
#                       """<h3>Please find the decryption error below </h3> 
#                       <br> {} <br>""".format(err, error_list),
#                       None, error_email_recipients, None)

    print("Renaming Decrypted Columns")
    decrypted_col_list = ['{}_decrypted'.format(cols) for cols in columns_to_be_decrypted]
    # Creating a new dataframe with decrypted columns suffix'd with '_decrypted'
    decrypted_df = pd.DataFrame(row_list, columns=decrypted_col_list)
    # Merging the main dataframe with decrypted dataframe using index as primary key
    print("Merging Original Dataframe with Decrypted Dataframe")
    merged_df = df.merge(decrypted_df, left_index=True, right_index=True)
    merged_df.drop(columns=columns_to_be_decrypted, axis=1, inplace=True)
    rename_col_dict = {}
    for col_val in columns_to_be_decrypted:
        rename_col_dict['{}_decrypted'.format(col_val)] = col_val
    # Drop the encrypted column and rename the decrypted column with the original name
    final_df = merged_df.rename(columns=rename_col_dict)
    # Arranging the result dataframe columns as like the original dataframe
    print("Re-arranging the column order as such as original dataframe")
    final_df = final_df[original_column_order_list]
    return final_df

def decrypt_cyphertext(df, columns_to_be_decrypted, integer_columns=[]):
    row_list = []
    original_column_order_list = list(df.columns)
    is_decrypt_error = False
    print("Decrypting Columns -> {}".format(str(columns_to_be_decrypted)))
    for row_ind, row_val in df.iterrows():
        col_list, error_list = [], []
        for col_val in columns_to_be_decrypted:
            # Iterate each row and each decrypting column in dataframe and decrypting only non-null values
            val = row_val[col_val]
            if type(val) == list:
                lst = []
                for each_val in val:
                    if each_val not in ['nan', 'Nan', 'NaN', ''] and isinstance(each_val, float) is False and each_val.isnumeric() is False and each_val is not None:
                        try:
                            decrypted_text = decryption(each_val)
                            lst.append(decrypted_text.decode('utf-8'))
                        except Exception as error:
                            is_decrypt_error = True
                            err = str(error)
                            print("**********",error)
                            #print("Decryption Error Occured in {} -> {}".format(col_val, each_val))
                            error_list.append("{} -> {}".format(col_val, each_val))
                            lst.append(None)
                    else:
                        lst.append(each_val)
                decrypted_val = str(lst)
            elif val not in ['nan', 'Nan', 'NaN', ''] and isinstance(val, float) is False and val is not None:
                try:
                    print("********",val)
                    if str(val).isnumeric() is False:
                        print("%%%%%%%")
                        decrypted_val = decryption(val)
                        print("^^^^^^^^^^^")
                        if col_val in integer_columns:
                            decrypted_val = struct.unpack('>q', decrypted_val)
                            decrypted_val = decrypted_val[0]
                        else:
                            decrypted_val = decrypted_val.decode('utf-8')
                    else:
                        print("00000")
                        decrypted_val = val
                    print("&&&&&&")
                except Exception as error:
                    is_decrypt_error = True
                    err = str(error)
                    print("--------",error)
                    print("Decryption Error Occured in {} -> {}".format(col_val, val))
                    error_list.append("{} -> {}".format(col_val, val))
                    decrypted_val = None
            else:
                decrypted_val = val
            col_list.append(decrypted_val)
        row_list.append(col_list)

    # if is_decrypt_error:
    #     print("Sending Decryption Error Email")
    #     email_wrapper('Decryption_Error_Email',
    #                   'Decryption Error',
    #                   """<h3>Please find the decryption error below </h3> 
    #                   <br> {} <br> {} br {}""".format(err, error_list, str(list(df.columns))),
    #                   None, error_email_recipients, None)

    print("Renaming Decrypted Columns")
    decrypted_col_list = ['{}_decrypted'.format(cols) for cols in columns_to_be_decrypted]
    # Creating a new dataframe with decrypted columns suffix'd with '_decrypted'
    decrypted_df = pd.DataFrame(row_list, columns=decrypted_col_list)
    # Merging the main dataframe with decrypted dataframe using index as primary key
    print("Merging Original Dataframe with Decrypted Dataframe")
    merged_df = df.merge(decrypted_df, left_index=True, right_index=True)
    merged_df.drop(columns=columns_to_be_decrypted, axis=1, inplace=True)
    rename_col_dict = {}
    for col_val in columns_to_be_decrypted:
        rename_col_dict['{}_decrypted'.format(col_val)] = col_val
    # Drop the encrypted column and rename the decrypted column with the original name
    final_df = merged_df.rename(columns=rename_col_dict)
    # Arranging the result dataframe columns as like the original dataframe
    print("Re-arranging the column order as such as original dataframe")
    final_df = final_df[original_column_order_list]
    return final_df