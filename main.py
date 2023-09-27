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
from db_recon_stats import insert_recon_stats,recon_stats_req
from db_exceptions import select_exceptions
from typing import List, Dict
from db_recon_data import update_reconciliation

# Log errors and relevant information using the Python logging module
import logging

reconciled_data = None
succunreconciled_data = None

app = FastAPI()

origins = [
    "*"
],
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"]
)

server = 'abcbusinessintelligence.database.windows.net'
database = 'BusinessIntelligence'
username = "isabiryed"
password = "Vp85FRFXYf2KBr@"

# Example usage for SELECT query:   
# connection_string = execute_query(server, database, username, password)
queryTst = "SELECT 1"
connection_string = execute_query(server, database, username, password,queryTst)

def use_cols(df):
    """
    Renames the 'Original_ABC Reference' column to 'Reference' and selects specific columns.

    :param df: DataFrame to be processed.
    :return: New DataFrame with selected and renamed columns.
    """
    df = df.rename(columns={'TXN_TYPE_y': 'TXN_TYPE', 'Original_TRN_REF': 'TRN_REF2'})

    # Convert 'DATE_TIME' to datetime
    df['DATE_TIME'] = pd.to_datetime(df['DATE_TIME'].astype(str), format='%Y%m%d')

    # Select only the desired columns
    selected_columns = ['DATE_TIME', 'AMOUNT', 'TRN_REF2', 'BATCH', 'TXN_TYPE', 
                        'ISSUER_CODE', 'ACQUIRER_CODE', 'RESPONSE_CODE', '_merge', 'Recon Status']
    df_selected = df[selected_columns]
    
    return df_selected

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

def process_reconciliation(DF1: pd.DataFrame, DF2: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame):
    
    # Rename columns of DF1 to match DF2 for easier merging
    DF1 = DF1.rename(columns={'Date': 'DATE_TIME','ABC Reference': 'TRN_REF','Amount': 'AMOUNT','Transaction type': 'TXN_TYPE'})
    
    # Merge the dataframes on the relevant columns
    merged_df = DF1.merge(DF2, on=['DATE_TIME', 'TRN_REF', 'AMOUNT'], how='outer', indicator=True)
    
    # Create a new column 'Recon Status'
    merged_df['Recon Status'] = 'Unreconciled'
    merged_df.loc[(merged_df['Recon Status'] == 'Unreconciled') & (merged_df['RESPONSE_CODE'] == '0') | (merged_df['Response_code'] == '0'), 'Recon Status'] = 'succunreconciled'
    merged_df.loc[merged_df['_merge'] == 'both', 'Recon Status'] = 'Reconciled'

    # Separate the data into three different dataframes based on the reconciliation status
    reconciled_data = merged_df[merged_df['Recon Status'] == 'Reconciled']
    succunreconciled_data = merged_df[merged_df['Recon Status'] == 'succunreconciled']
    unreconciled_data = merged_df[merged_df['Recon Status'] == 'Unreconciled']
    exceptions = merged_df[(merged_df['Recon Status'] == 'Reconciled') & (merged_df['RESPONSE_CODE'] != '0')]

    return merged_df, reconciled_data, succunreconciled_data, exceptions

def unserializable_floats(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace({math.nan: "NaN", math.inf: "Infinity", -math.inf: "-Infinity"})
    return df
    
def main(path,Swift_code_up):

    global reconciled_data, succunreconciled_data  # Indicate these are global variables
    
    # Read the uploaded dataset from Excel
    uploaded_df = pd.read_excel(path , usecols = [0, 1, 2, 3], skiprows = 0)

    # Now, you can use strftime to format the 'Date' column
    min_date, max_date = date_range(uploaded_df, 'Date')

    date_range_str = f"{min_date},{max_date}"

    uploaded_df = backup_refs(uploaded_df, 'ABC Reference')
    uploaded_df['Response_code'] = '0'
    UploadedRows = len(uploaded_df)
    # Clean and format columns in the uploaded dataset
    # Apply the data_pre_processing function to the uploaded_df dataframe
    uploaded_df_processed = pre_processing(uploaded_df)
        
    # Define the SQL query
    query = f"""
        SELECT DISTINCT DATE_TIME, BATCH,TRN_REF, TXN_TYPE, ISSUER_CODE, ACQUIRER_CODE,
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
        requestedRows = len(datadump[datadump['RESPONSE_CODE'] == '0'])

        # Clean and format columns in the datadump        
        # Apply the data_pre_processing function to the datadump dataframe
        db_preprocessed = pre_processing(datadump)
        
        # Now, you can use strftime to format the 'DATE_TIME' column if needed        
                
        merged_df, reconciled_data,succunreconciled_data, exceptions = process_reconciliation(uploaded_df_processed,db_preprocessed)  
        succunreconciled_data = use_cols (succunreconciled_data) 
        reconciled_data = use_cols (reconciled_data) 

        feedback  = update_reconciliation(reconciled_data,server, database, username, password,Swift_code_up)      
         
        insert_recon_stats(Swift_code_up,Swift_code_up,len(reconciled_data),len(succunreconciled_data),len(exceptions),feedback,(requestedRows),(UploadedRows),
           date_range_str,server,database,username,password) 
        
        logging.basicConfig(filename = 'reconciliation.log', level = logging.ERROR)
        try:
            
            print('Thank you, your reconciliation is complete. ' + feedback)
            
            pass
        except Exception as e:
            logging.error(f"Error: {str(e)}")

        return merged_df,reconciled_data,succunreconciled_data,exceptions,feedback,requestedRows,UploadedRows,date_range_str

@app.post("/reconcile")
async def reconcile(file: UploadFile = File(...), swift_code: str = Form(...)):
    
    # Save the uploaded file temporarily
    
    temp_file_path = "temp_file.xlsx"
    with open(temp_file_path, "wb") as buffer:
        buffer.write(file.file.read())
    
    try:
        # Call the main function with the path of the saved file and the swift code
        merged_df, reconciled_data, succunreconciled_data, exceptions,feedback,requestedRows,UploadedRows,date_range_str = main(temp_file_path, swift_code)
        reconciledRows = len(reconciled_data)
        unreconciledRows = len(succunreconciled_data)
        exceptionsRows = len(exceptions)
        # Clean up: remove the temporary file after processing
        os.remove(temp_file_path)
        
        data =  {
           
            "reconciledRows": reconciledRows,
            "unreconciledRows": unreconciledRows,
            "exceptionsRows": exceptionsRows,
            "feedback": feedback,
            "RequestedRows":requestedRows,
            "UploadedRows":UploadedRows,
            "min_max_DateRange":date_range_str
        }

        json_data = json.dumps(data,indent = 4)
        return json_data
    
    except Exception as e:
        # If there's an error during the process, ensure the temp file is removed
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        
        # Raise a more specific error to FastAPI to handle
        raise HTTPException(status_code = 500, detail = str(e))  

@app.get("/reconstats")
async def getReconStats(Swift_code_up: str):
    data = recon_stats_req(server, database, username, password, Swift_code_up)
    df = pd.DataFrame(data)
    # Convert DataFrame to a list of dictionaries for JSON serialization
    return df.to_dict(orient='records')

@app.get("/exceptions")
async def getExceptions(Swift_code_up: str):
    data = select_exceptions(server, database, username, password, Swift_code_up)
    df = pd.DataFrame(data)
    # Convert DataFrame to a list of dictionaries for JSON serialization
    return df.to_dict(orient='records')

@app.get("/reconcileddata")
async def get_reconciled_data():
    global reconciled_data
    if reconciled_data is not None:
        reconciled_data_cleaned = unserializable_floats(reconciled_data)
        return reconciled_data_cleaned.to_dict(orient='records')
    else:
        raise HTTPException(status_code = 404, detail="Reconciled data not found")

@app.get("/unreconcileddata")
async def get_unreconciled_data():
    global succunreconciled_data
    if succunreconciled_data is not None:
        unreconciled_data_cleaned = unserializable_floats(succunreconciled_data)
        return unreconciled_data_cleaned.to_dict(orient='records')
    else:
        raise HTTPException(status_code = 404, detail="Unreconciled data not found")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host = "0.0.0.0", port = 8000)

    