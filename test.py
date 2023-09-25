import pandas as pd
import re
import math
import pyodbc
from openpyxl.utils.dataframe import dataframe_to_rows
from db_connect import execute_query
from db_update import batch_update
from fastapi.middleware.cors import CORSMiddleware
import json
import os
from fastapi import FastAPI, Query, UploadFile, Form,File,HTTPException
from db_reconcile import insert_recon_stats,recon_stats_req
from db_exceptions import select_exceptions
Swift_code_up = 130447

# Log errors and relevant information using the Python logging module
import logging


server = 'abcbusinessintelligence.database.windows.net'
database = 'BusinessIntelligence'
username = "isabiryed"
password = "Vp85FRFXYf2KBr@"

# Example usage for SELECT query:   
# connection_string = execute_query(server, database, username, password)
queryTst = "SELECT 1"
connection_string = execute_query(server, database, username, password,queryTst)



def process_reconciliation(DF1: pd.DataFrame, DF2: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame):
    
    # Rename columns of DF1 to match DF2 for easier merging
    DF1 = DF1.rename(columns={
        'Date': 'DATE_TIME',
        'ABC Reference': 'TRN_REF',
        'Amount': 'AMOUNT'
        
    })
    
    # Merge the dataframes on the relevant columns
    merged_df = DF1.merge(DF2, on=['DATE_TIME', 'TRN_REF', 'AMOUNT'], how='outer', indicator=True)
    
    # Create a new column 'Recon Status'
    merged_df['Recon Status'] = 'Unreconciled'
    
    # Update the 'Recon Status' column based on conditions
    merged_df.loc[(merged_df['Recon Status'] == 'Unreconciled') & (merged_df['RESPONSE_CODE'] == '0') | (merged_df['Response_code'] == '0'), 'Recon Status'] = 'succunreconciled'
    merged_df.loc[merged_df['_merge'] == 'both', 'Recon Status'] = 'Reconciled'

    # Separate the data into three different dataframes based on the reconciliation status
    reconciled_data = merged_df[merged_df['Recon Status'] == 'Reconciled']
    succunreconciled_data = merged_df[merged_df['Recon Status'] == 'succunreconciled']
    unreconciled_data = merged_df[merged_df['Recon Status'] == 'Unreconciled']
    exceptions = merged_df[(merged_df['Recon Status'] == 'Reconciled') & (merged_df['RESPONSE_CODE'] != '0')]

    return merged_df, reconciled_data,succunreconciled_data,unreconciled_data, exceptions

def backup_refs(df, reference_column):
    # Backup the original reference column
    df['Original_' + reference_column] = df[reference_column]
    
    return df

def date_range(dataframe, date_column):
    min_date = dataframe[date_column].min().strftime('%Y-%m-%d')
    max_date = dataframe[date_column].max().strftime('%Y-%m-%d')
    return min_date, max_date

def pre_processing(df):
    # Helper functions
    def clean_amount(value):
        try:
            # Convert the value to a float and then to an integer to remove decimals
            return str(int(float(value)))
        except:
            return '0'  # Default to '0' if conversion fails
    
    def remo_spec_x(value):
        cleaned_value = re.sub(r'[^0-9a-zA-Z]', '', str(value))
        if cleaned_value == '':
            return '0'
        return cleaned_value
    
    def pad_strings_with_zeros(input_str):
        if len(input_str) < 12:
            num_zeros = 12 - len(input_str)
            padded_str = '0' * num_zeros + input_str
            return padded_str
        else:
            return input_str[:12]

    def clean_date(value):
        try:
            # Convert to datetime to ensure it's in datetime format
            date_value = pd.to_datetime(value).date()
            return str(date_value).replace("-", "")
        except:
            return value  # Return the original value if conversion fails

    # Cleaning logic
    for column in df.columns:
        # Cleaning for date columns
        if column in ['Date', 'DATE_TIME']:
            df[column] = df[column].apply(clean_date)
        # Cleaning for amount columns
        elif column in ['Amount', 'AMOUNT']:
            df[column] = df[column].apply(clean_amount)
        else:
            df[column] = df[column].apply(remo_spec_x)  # Clean without converting to string
        
        # Padding for specific columns
        if column in ['ABC Reference', 'TRN_REF']:
            df[column] = df[column].apply(pad_strings_with_zeros)
    
    return df

# Read the uploaded dataset from Excel
path = r'C:\Users\dataanalyst\Desktop\Python projects\datasets'
uploaded_df = pd.read_excel(path + r'\testupload_.xlsx', usecols = [0, 1, 2, 3], skiprows = 0)

# Now, you can use strftime to format the 'Date' column
min_date, max_date = date_range(uploaded_df, 'Date')

date_range_str = f"{min_date},{max_date}"

uploaded_df = backup_refs(uploaded_df, 'ABC Reference')

uploaded_df['Response_code'] = '0'
UploadedRows = len(uploaded_df)
# Clean and format columns in the uploaded dataset
# Apply the data_pre_processing function to the uploaded_df dataframe
up_preprocessing_result = pre_processing(uploaded_df)

     
# Define the SQL query
query = f"""
        SELECT DISTINCT DATE_TIME, TRN_REF, TXN_TYPE, ISSUER_CODE, ACQUIRER_CODE,
               AMOUNT, RESPONSE_CODE
        FROM Transactions
        WHERE (ISSUER_CODE = '{Swift_code_up}' OR ACQUIRER_CODE = '{Swift_code_up}')
            AND CONVERT(DATE, DATE_TIME) BETWEEN '{min_date}' AND '{max_date}'
            AND REQUEST_TYPE NOT IN ('1420','1421')
            AND TXN_TYPE NOT IN ('ACI','AGENTFLOATINQ','BI','MINI')

    """
    
    # Execute the SQL query
datadump = execute_query(server, database, username, password,query, query_type = "SELECT")
    
if datadump is not None:
        datadump = backup_refs(datadump, 'TRN_REF')
        requestedRows = len(datadump)
        # Clean and format columns in the datadump        
        # Apply the data_pre_processing function to the datadump dataframe
        db_preprocessing_result = pre_processing(datadump)
               
        # Merge the dataframes
        merged_df, reconciled_data,succunreconciled_data,unreconciled_data, exceptions = process_reconciliation(uploaded_df,datadump)
        # merged_df = reconcile_dataframes(uploaded_df, datadump)
        # merged_df = datadump.merge(uploaded_df, left_on='concat_db', right_on='concat_up', how='outer')
        # Use the function to classify and filter rows
        # reconciled_data, unreconciled_data, exceptions = process_reconciliation(merged_df, columns_to_select)        
        print(merged_df.head(20))
        # print(datadump.head(20))
#         # # The batch_update, feedback on update functions!
#         # dbupdate = batch_update(reconciled_data, Swift_code_up, min_date, max_date, server, database, username, password, execute_query)
#         # feedback, post_update_count = batch_update(reconciled_data, Swift_code_up, min_date, max_date, server, database,
#         #                                             username, password, execute_query)  
        
#         # insert_recon_stats(Swift_code_up,Swift_code_up,len(reconciled_data),len(unreconciled_data),len(exceptions),feedback,(requestedRows),(UploadedRows),
#         #    date_range_str,server,database,username,password) 

#         logging.basicConfig(filename = 'reconciliation.log', level = logging.ERROR)
#         try:
            
#             print('Thank you, your reconciliation is complete. ' + feedback)
            
#             pass
#         except Exception as e:
#             logging.error(f"Error: {str(e)}")



    
    
    
