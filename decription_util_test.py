from airflow.configuration import conf
from airflow.models import Variable
from vivriti.utils.email_utils import email_wrapper

import boto3
import pandas as pd
import struct
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from base64 import b64decode

error_email_recipients = Variable.get("decrypt_error_email")

kms_client = boto3.client('kms', region_name='ap-south-1')
encrypted_master_key = conf.get('colend', 'encryptionkey')
hex_encrypted_master_key = bytes.fromhex(encrypted_master_key)
decrypted_master_key = kms_client.decrypt(CiphertextBlob=hex_encrypted_master_key)
master_key = decrypted_master_key['Plaintext'].decode()


def decryption(ciphertext):
    ciphertext = b64decode(ciphertext)
    key = bytes.fromhex(master_key)
    aesgcm = AESGCM(key)
    decrypted_text = aesgcm.decrypt(ciphertext[:12], ciphertext[12:], b'')
    return decrypted_text


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
                            print("Decryption Error Occured in {} -> {}".format(col_val, each_val))
                            error_list.append("{} -> {}".format(col_val, each_val))
                            lst.append(None)
                    else:
                        lst.append(each_val)
                decrypted_val = str(lst)
            elif val not in ['nan', 'Nan', 'NaN', ''] and isinstance(val, float) is False and val is not None:
                try:
                    if val.isnumeric() is False:
                        decrypted_val = decryption(val)
                        if col_val in integer_columns:
                            decrypted_val = struct.unpack('>q', decrypted_val)
                            decrypted_val = decrypted_val[0]
                        else:
                            decrypted_val = decrypted_val.decode('utf-8')
                    else:
                        decrypted_val = val
                except Exception as error:
                    is_decrypt_error = True
                    err = str(error)
                    print("Decryption Error Occured in {} -> {}".format(col_val, val))
                    error_list.append("{} -> {}".format(col_val, val))
                    decrypted_val = None
            else:
                decrypted_val = val
            col_list.append(decrypted_val)
        row_list.append(col_list)

    if is_decrypt_error:
        print("Sending Decryption Error Email")
        email_wrapper('Decryption_Error_Email',
                      'Decryption Error',
                      """<h3>Please find the decryption error below </h3> 
                      <br> {} <br> {} br {}""".format(err, error_list, str(list(df.columns))),
                      None, error_email_recipients, None)

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