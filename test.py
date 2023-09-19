import pandas as pd
import re
import math
import pyodbc
from openpyxl.utils.dataframe import dataframe_to_rows
from db_connect_aws import execute_query
from db_update import batch_update
from fastapi.middleware.cors import CORSMiddleware
import json
import os
from fastapi import FastAPI, UploadFile, Form,File,HTTPException
# Log errors and relevant information using the Python logging module
import logging

app = FastAPI()

# origins = [
#     "*"
# ],
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=origins,
#     allow_methods=["*"],
#     allow_headers=["*"]
# )

server = 'businessintelligence.cqo7jevz1qxm.eu-north-1.rds.amazonaws.com,1433'
database = 'BusinessIntelligence'
username = "isabiryed"
password = "Tech247w247"

def backup_refs(df, reference_column):
    # Backup the original reference column
    df['Original_' + reference_column] = df[reference_column]
    
    return df

def concat_column(dataframe, base_name, date_column, reference_column, amount_column):
    # Create a concatenated column with the format: concat_<base_name>
    column_name = f'concat_{base_name}'
    dataframe[column_name] = (
        dataframe[date_column].astype(str) +
        dataframe[reference_column].astype(str) +
        dataframe[amount_column].astype(str)
    )
    return dataframe

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

#     return reconciled_data, unreconciled_data, exceptions
def insert_reconciliation(reconciledRows, unreconciledRows, exceptionsRows, feedback, requestedRows, UploadedRows, date_range_str, server, database, username, password):
    # Define the SQL query for insertion
    insert_query = f"""
        INSERT INTO Reconciliation
        (RECON_RWS, UNRECON_RWS, EXCEP_RWS, FEEDBACK, RQ_RWS, UPLD_RWS, RQ_DATE_RANGE)
        VALUES
        ({reconciledRows}, {unreconciledRows}, {exceptionsRows}, '{feedback}', {requestedRows}, {UploadedRows}, '{date_range_str}')
    """
    
    # Execute the SQL query
    execute_query(server, database, username, password, insert_query, query_type="INSERT")

def select_reconciliation_data(server, database, username, password,swift_code):
    # Define the SQL query for selection
    select_query = f"""
            SELECT RQ_RWS, RQ_DATE_RANGE, UPLD_RWS, EXCEP_RWS, RECON_RWS, UNRECON_RWS, FEEDBACK 
            FROM Reconciliation WHERE BANK_ID = '{swift_code}'
        """    
    # Execute the SQL query and retrieve the results
    recon_results_df = execute_query(server, database, username, password, select_query, query_type="SELECT")
    
    return recon_results_df

def process_reconciliation(dataframe, response_column, columns_to_select, additional_check_column=None):
    # Create a new 'Recon Status' column
    dataframe['Recon Status'] = 'Unreconciled'
    
    # Check the specified columns for non-NaN values
    if additional_check_column:
        dataframe.loc[(dataframe['Recon Status'].notna()) & (dataframe[additional_check_column].notna()), 'Recon Status'] = 'Reconciled'
    else:
        dataframe.loc[dataframe['Recon Status'].notna(), 'Recon Status'] = 'Reconciled'

    # Separate reconciled and unreconciled rows based on conditions
    reconciled_data = dataframe[
        (dataframe['Recon Status'] == 'Reconciled')
    ][columns_to_select]
    
    unreconciled_data = dataframe[
        dataframe['Recon Status'] == 'Unreconciled'
    ][columns_to_select]

    # Rows that are reconciled but have a non-zero RESPONSE CODE
    exceptions = dataframe[
        (dataframe['Recon Status'] == 'Reconciled') &
        (dataframe[response_column] != '0')
    ][columns_to_select]

    return reconciled_data, unreconciled_data, exceptions

def main(path,Swift_code_up):
    # Example usage for SELECT query:
    
    
    # connection_string = execute_query(server, database, username, password)
    queryTst = "SELECT 1"
    connection_string = execute_query(server, database, username, password,queryTst)

    # Read the uploaded dataset from Excel
    uploaded_df = pd.read_excel(path , usecols = [0, 1, 2, 3], skiprows = 0)

    # Now, you can use strftime to format the 'Date' column
    min_date, max_date = date_range(uploaded_df, 'Date')

    date_range_str = f"{min_date},{max_date}"

    uploaded_df = backup_refs(uploaded_df, 'ABC Reference')
    UploadedRows = len(uploaded_df)
    # Clean and format columns in the uploaded dataset
    # Apply the data_pre_processing function to the uploaded_df dataframe
    up_preprocessing_result = pre_processing(uploaded_df)
    uploaded_df = concat_column(up_preprocessing_result,'up', 'Date', 'ABC Reference', 'Amount')
    
    # Define the SQL query
    query = f"""
        SELECT DATE_TIME, TRN_REF, TXN_TYPE, ISSUER_CODE, ACQUIRER_CODE,
               AMOUNT, RESPONSE_CODE
        FROM Transactions
        WHERE (ISSUER_CODE = '{Swift_code_up}' OR ACQUIRER_CODE = '{Swift_code_up}')
            AND CONVERT(DATE, DATE_TIME) BETWEEN '{min_date}' AND '{max_date}'
    """

    # Execute the SQL query
    datadump = execute_query(server, database, username, password,query, query_type = "SELECT")
    
    if datadump is not None:
        datadump = backup_refs(datadump, 'TRN_REF')
        requestedRows = len(datadump)
        # Clean and format columns in the datadump        
        # Apply the data_pre_processing function to the datadump dataframe
        db_preprocessing_result = pre_processing(datadump)
        datadump = concat_column(db_preprocessing_result,'db', 'DATE_TIME', 'TRN_REF', 'AMOUNT')
        # Now, you can use strftime to format the 'DATE_TIME' column if needed
        
        # Merge and analyze data
        merged_df = datadump.merge(uploaded_df, left_on='concat_db', right_on='concat_up', how='outer')  
        
        # Define columns to select for the separated dataframes
        columns_to_select = ['Date','Original_ABC Reference', 'ABC Reference', 'Amount', 'Transaction type','Original_TRN_REF', 'TRN_REF', 'DATE_TIME', 'TXN_TYPE', 'ISSUER_CODE', 'ACQUIRER_CODE', 'RESPONSE_CODE','Recon Status']
        
        reconciled_data, unreconciled_data, exceptions = process_reconciliation(
                dataframe=merged_df,
                response_column='RESPONSE_CODE',
                columns_to_select=columns_to_select,
                additional_check_column='TRN_REF'
            )
        
        # The batch_update, feedback on update functions!
        dbupdate = batch_update(reconciled_data, Swift_code_up, min_date, max_date, server, database, username, password, execute_query)
        feedback, post_update_count = batch_update(reconciled_data, Swift_code_up, min_date, max_date, server, database, username, password, execute_query)  
        
        insert_reconciliation(
           len(reconciled_data),
           len(unreconciled_data),
           len(exceptions),
           feedback,
           (requestedRows),
           (UploadedRows),
           date_range_str,
           server,database,username,password
           )  
        

        logging.basicConfig(filename = 'reconciliation.log', level = logging.ERROR)
        try:
            #print (expected_rowcount)
            #print (updated_rows)
            print('Thank you, your reconciliation is complete. ' + feedback)
            
            pass
        except Exception as e:
            logging.error(f"Error: {str(e)}")

        return merged_df,reconciled_data,unreconciled_data,exceptions,feedback,requestedRows,UploadedRows,date_range_str

@app.post("/reconcile")
async def reconcile(file: UploadFile = File(...), swift_code: str = Form(...)):
    
    # Save the uploaded file temporarily
    # path = r'C:\Users\ISABIRYEDICKSON\Desktop\marvin trials\FastApi'
    # temp_file_path = os.path.join(path, 'temp_file.xlsx')
    temp_file_path = "temp_file.xlsx"
    with open(temp_file_path, "wb") as buffer:
        buffer.write(file.file.read())
    
    try:
        # Call the main function with the path of the saved file and the swift code
        merged_df, reconciled_data, unreconciled_data, exceptions,feedback,requestedRows,UploadedRows,date_range_str= main(temp_file_path, swift_code)
        reconciledRows = len(reconciled_data)
        unreconciledRows = len(unreconciled_data)
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

        json_data = json.dumps(data,indent=4)
        return json_data
    
    except Exception as e:
        # If there's an error during the process, ensure the temp file is removed
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        
        # Raise a more specific error to FastAPI to handle
        raise HTTPException(status_code=500, detail=str(e))
  

# @app.get("/ReconStats")
# async def getReconStats():
data = select_reconciliation_data(server,database,username,password,130447)
print(data)
# result = json.dumps(data)
#     return  data

# @app.get("/Exceptions")
# async def getExceptions():
#     data = select_reconciliation_data(server,database,username,password)
#     result = json.dumps(data)
#     return  result


# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)