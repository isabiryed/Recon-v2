import os
import glob
import pandas as pd
import logging
from fastapi import FastAPI, Query, UploadFile, Form,File,HTTPException
import json

from main import pre_processing
from db_settle import select_setle_file
from setle_main import batch

server = 'abcbusinessintelligence.database.windows.net'
database = 'BusinessIntelligence'
username = "isabiryed"
password = "Vp85FRFXYf2KBr@"

batch = 2349

path = rf'\Users\ISABIRYEDICKSON\Desktop\Python projects\datasets\Batches\Batch {batch}.xlsx'

def process_reconciliation(DF1: pd.DataFrame, DF2: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    
    # Merge the dataframes on the relevant columns
    merged_df = DF1.merge(DF2, on=['DATE_TIME', 'TRN_REF'], how='outer', suffixes=('_DF1', '_DF2'), indicator=True)
    
        # Now perform the subtraction
    merged_df.loc[merged_df['_merge'] == 'both', 'AMOUNT_DIFF'] = (
        pd.to_numeric(merged_df['AMOUNT_DF1'], errors='coerce') - 
        pd.to_numeric(merged_df['AMOUNT_DF2'], errors='coerce')
    )

    merged_df.loc[merged_df['_merge'] == 'both', 'ABC_COMMISSION_DIFF'] = (
        pd.to_numeric(merged_df['ABC_COMMISSION_DF1'], errors='coerce') - 
        pd.to_numeric(merged_df['ABC_COMMISSION_DF2'], errors='coerce')
    )
    
    # Create a new column 'Recon Status'
    merged_df['Recon Status'] = 'Unreconciled'    
    merged_df.loc[merged_df['_merge'] == 'both', 'Recon Status'] = 'Reconciled'
    
    # Separate the data into different dataframes based on the reconciliation status
    reconciled_data = merged_df[merged_df['Recon Status'] == 'Reconciled']
    unreconciled_data = merged_df[merged_df['Recon Status'] == 'Unreconciled']
    setle_sabs_reconcile_data = merged_df[(merged_df['AMOUNT_DIFF'] != 0) | (merged_df['ABC_COMMISSION_DIFF'] != 0)]
    
    # Define the columns to keep for merged_df
    use_columns = ['TRN_REF', 'DATE_TIME', 'BATCH_DF1', 'TXN_TYPE_DF1', 'AMOUNT_DF1', 
                            'FEE_DF1', 'ABC_COMMISSION_DF1', 'AMOUNT_DIFF', 'ABC_COMMISSION_DIFF', 
                            '_merge', 'Recon Status']

    # Select only the specified columns for merged_df
    merged_df = merged_df.loc[:, use_columns]    
    reconciled_data = reconciled_data.loc[:, use_columns]
    unreconciled_data = unreconciled_data.loc[:, use_columns]
    setle_sabs_reconcile_data = setle_sabs_reconcile_data.loc[:, use_columns]

    return merged_df, reconciled_data, unreconciled_data,setle_sabs_reconcile_data


def read_excel_file(file_path, sheet_name):
    try:
        with pd.ExcelFile(file_path) as xlsx:
            df = pd.read_excel(xlsx, sheet_name=sheet_name, usecols=[0, 1, 2, 7, 8, 9, 11], skiprows=0)
        # Rename the columns
        df.columns = ['TRN_REF', 'DATE_TIME', 'BATCH', 'TXN_TYPE', 'AMOUNT', 'FEE', 'ABC_COMMISSION']
        return df
    except Exception as e:
        logging.error(f"An error occurred while opening the Excel file: {e}")
        return None

def pre_processing_amt(df):
    # Helper function
    def clean_amount(value):
        try:
            # Convert the value to a float, round to nearest integer
            return round(float(value))  # round the value and return as integer
        except:
            return value  # Return the original value if conversion fails
    
    # Cleaning logic
    for column in ['AMOUNT', 'FEE', 'ABC_COMMISSION']:  # only these columns
        df[column] = df[column].apply(clean_amount)
    
    return df
    
def main(path,batch):
    datadump = select_setle_file(server, database, username, password, batch)
    
    # Check if datadump is not None and not empty
    if datadump is not None and not datadump.empty:         
        datadump = pre_processing_amt(datadump)
        datadump = pre_processing(datadump)
        # print(datadump.head(10))
    else:
        print("No records for processing found.")

    # Processing SABSfile_ regardless of datadump's status
    excel_files = glob.glob(path)
    if not excel_files:
        logging.error(f"No matching Excel file found for '{path}'.")
    else:
        matching_file = excel_files[0]
        SABSfile_ = read_excel_file(matching_file, 'Transaction Report')
        SABSfile_ = pre_processing_amt(SABSfile_)
        SABSfile_ = pre_processing(SABSfile_)
        # print(SABSfile_.head(10))
    
    merged_df, reconciled_data,unreconciled_data,setle_sabs_reconcile_data = process_reconciliation(SABSfile_,datadump)
    # print(setle_sabs_reconcile_data.head(10))

    logging.basicConfig(filename = 'settlement.log', level = logging.ERROR)
    try:
            
        print('Thank you, your settlement Report is ready')
            
        pass
    except Exception as e:
        logging.error(f"Error: {str(e)}")

    return merged_df,reconciled_data,unreconciled_data,setle_sabs_reconcile_data

main(path,batch)

@app.post("/sabsreconcile")
async def sabsreconcile(file: UploadFile = File(...), batch_number: str = Form(...)):
    
    # Save the uploaded file temporarily    
    temp_file_path = "temp_file.xlsx"
    with open(temp_file_path, "wb") as buffer:
        buffer.write(file.file.read())
    
    try:
        # Call the main function with the path of the saved file and the swift code
        reconciled_data, setle_sabs_reconcile_data = main(temp_file_path, batch_number)
        reconciledrows = len(reconciled_data)
        unreconciledrows = len(setle_sabs_reconcile_data)
        
        # Clean up: remove the temporary file after processing
        os.remove(temp_file_path)
        
        data =  {
           
            "reconciledrows": reconciledrows,
            "unreconciledrows": unreconciledrows,
            
        } 

        json_data = json.dumps(data,indent = 4)
        return json_data
    
    except Exception as e:
        # If there's an error during the process, ensure the temp file is removed
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        
        # Raise a more specific error to FastAPI to handle
        raise HTTPException(status_code = 500, detail = str(e))  

@app.get("/reconcileddata")
async def get_reconciled_data():
    global reconciled_data
    if reconciled_data is not None:
        reconciled_data_cleaned = unserializable_floats(reconciled_data)
        return reconciled_data_cleaned.to_dict(orient='records')
    else:
        raise HTTPException(status_code = 404, detail="Reconciled data not found")

