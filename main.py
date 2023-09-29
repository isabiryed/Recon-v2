import pandas as pd
import re
import math
import pyodbc
from openpyxl.utils.dataframe import dataframe_to_rows
from db_connect import execute_query
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import StreamingResponse
from zipfile import ZipFile
from io import BytesIO
import json
import os
from fastapi import FastAPI, Query, UploadFile, Form,File,HTTPException
from db_recon_stats import insert_recon_stats,recon_stats_req
from db_exceptions import select_exceptions
from db_reversals import select_reversals
from typing import List, Dict
from db_recon_data import update_reconciliation

# Log errors and relevant information using the Python logging module
import logging

from setle_sabs import pre_processing, setleSabs
from setlement_ import settle

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
            AND (A.AMOUNT > 0 OR A.AMOUNT < 0)
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

@app.get("/reversals")
async def getreversals(Swift_code_up: str):
    data = select_reversals(server, database, username, password, Swift_code_up)
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

@app.post("/sabsreconcile/csv_files/")
async def sabsreconcile_csv_files(file: UploadFile = File(...), batch_number: str = Form(...)):
    
    temp_file_path = "temp_file.xlsx"
    with open(temp_file_path, "wb") as buffer:
        buffer.write(file.file.read())

    try:
        # Assume setleSabs returns dataframes as one of its outputs
        _, matched_setle, _, unmatched_setlesabs = setleSabs(temp_file_path, batch_number)
        os.remove(temp_file_path)

        matched_csv = matched_setle.to_csv(index=False)
        unmatched_csv = unmatched_setlesabs.to_csv(index=False)
        
        # Create a zip file in memory
        memory_file = BytesIO()
        with ZipFile(memory_file, 'w') as zf:
            zf.writestr('matched_setle.csv', matched_csv)
            zf.writestr('unmatched_setlesabs.csv', unmatched_csv)

        memory_file.seek(0)
        
        return StreamingResponse(memory_file, media_type="application/zip", headers={"Content-Disposition": "attachment; filename=Settlement_recon_files.zip"})

    except Exception as e:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/settlementcsv_files/")
async def settlement_csv_files(batch_number: str = Form(...)):

    try:
        # Assume the settle function is defined and available here
        setlement_result = settle(batch_number)
        
        # Handle case where no records were found or an error occurred in settle
        if setlement_result is None or setlement_result.empty:
            raise HTTPException(status_code= 400, detail="No records for processing found or an error occurred.")

        # Convert the DataFrame to CSV
        setlement_csv = setlement_result.to_csv(index=False)
                
        # Create a zip file in memory
        memory_file = BytesIO()
        with ZipFile(memory_file, 'w') as zf:
            zf.writestr('setlement_result.csv', setlement_csv)
            
        memory_file.seek(0)
        
        # Return the zip file as a response
        return StreamingResponse(
            memory_file, 
            media_type="application/zip", 
            headers={"Content-Disposition": "attachment; filename=Settlement_.zip"}
        )

    except Exception as e:
        # Handle other unexpected errors
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host = "0.0.0.0", port = 8000)

    