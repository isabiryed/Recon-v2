import os
import glob
import pandas as pd
import logging
from fastapi import FastAPI, Query, UploadFile, Form,File,HTTPException
import json
import math
import re

from db_settle_recon import select_setle_file
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Query, UploadFile, Form,File,HTTPException
from setlement_ import batch

server = 'abcbusinessintelligence.database.windows.net'
database = 'BusinessIntelligence'
username = "isabiryed"
password = "Vp85FRFXYf2KBr@"

# batch = 2349
# path = rf'\Users\ISABIRYEDICKSON\Desktop\Python projects\datasets\Batches\Batch {batch}.xlsx'

def unserializable_floats(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace({math.nan: "NaN", math.inf: "Infinity", -math.inf: "-Infinity"})
    return df

def process_reconciliation(DF1: pd.DataFrame, DF2: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    
    # Merge the dataframes on the relevant columns
    merged_setle = DF1.merge(DF2, on=['DATE_TIME', 'TRN_REF'], how='outer', suffixes=('_DF1', '_DF2'), indicator=True)
    
        # Now perform the subtraction
    merged_setle.loc[merged_setle['_merge'] == 'both', 'AMOUNT_DIFF'] = (
        pd.to_numeric(merged_setle['AMOUNT_DF1'], errors='coerce') - 
        pd.to_numeric(merged_setle['AMOUNT_DF2'], errors='coerce')
    )

    merged_setle.loc[merged_setle['_merge'] == 'both', 'ABC_COMMISSION_DIFF'] = (
        pd.to_numeric(merged_setle['ABC_COMMISSION_DF1'], errors='coerce') - 
        pd.to_numeric(merged_setle['ABC_COMMISSION_DF2'], errors='coerce')
    )
    
    # Create a new column 'Recon Status'
    merged_setle['Recon Status'] = 'Unreconciled'    
    merged_setle.loc[merged_setle['_merge'] == 'both', 'Recon Status'] = 'Reconciled'
    
    # Separate the data into different dataframes based on the reconciliation status
    matched_setle = merged_setle[merged_setle['Recon Status'] == 'Reconciled']
    unmatched_setle = merged_setle[merged_setle['Recon Status'] == 'Unreconciled']
    unmatched_setlesabs = merged_setle[(merged_setle['AMOUNT_DIFF'] != 0) | (merged_setle['ABC_COMMISSION_DIFF'] != 0)]
    
    # Define the columns to keep for merged_setle
    use_columns = ['TRN_REF', 'DATE_TIME', 'BATCH_DF1', 'TXN_TYPE_DF1', 'AMOUNT_DF1', 
                            'FEE_DF1', 'ABC_COMMISSION_DF1', 'AMOUNT_DIFF', 'ABC_COMMISSION_DIFF', 
                            '_merge', 'Recon Status']

    # Select only the specified columns for merged_setle
    merged_setle = merged_setle.loc[:, use_columns]    
    matched_setle = matched_setle.loc[:, use_columns]
    unmatched_setle = unmatched_setle.loc[:, use_columns]
    unmatched_setlesabs = unmatched_setlesabs.loc[:, use_columns]

    return merged_setle, matched_setle, unmatched_setle,unmatched_setlesabs

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

def setleSabs(path,batch):
    
    try:
     
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
        
        merged_setle, matched_setle,unmatched_setle,unmatched_setlesabs = process_reconciliation(SABSfile_,datadump)
        # print(unmatched_setlesabs.head(10))

        logging.basicConfig(filename = 'settlement_recon.log', level = logging.ERROR)
            
        print('Thank you, your settlement Report is ready')
        # pass
    except Exception as e:
        logging.error(f"Error: {str(e)}")

    return merged_setle,matched_setle,unmatched_setle,unmatched_setlesabs

