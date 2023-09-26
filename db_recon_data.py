import pandas as pd
import logging
from db_connect import execute_query
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.ERROR, filename='reconciliation.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def update_reconciliation(df, server, database, username, password, swift_code):
    if df.empty:
        logging.error("The df DataFrame is empty.")
        return

    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    temp_except_flag = 'y'

    for index, row in df.iterrows():
        date_time = datetime.strptime(row['DATE_TIME'], '%Y%m%d')
        trn_ref = row['Original_ABC Reference']
        issuer_code = row['ISSUER_CODE']
        acquirer_code = row['ACQUIRER_CODE']

        if pd.isnull(trn_ref):
            logging.error(f"TRN_REF is empty for index: {index}")
            continue

        acq_flg = 1 if acquirer_code == swift_code else 0 
        iss_flg = 1 if issuer_code == swift_code else 0 
        acq_flg_date = current_datetime if acquirer_code == swift_code else 0 
        iss_flg_date = current_datetime if issuer_code == swift_code else 0 

        select_query = f"SELECT * FROM reconciliation WHERE TRN_REF = '{trn_ref}'"
        existing_data = execute_query(server, database, username, password, select_query, query_type="SELECT")
        if not existing_data.empty:
            update_query = f"""
                UPDATE reconciliation 
                SET 
                    TRAN_DATE = '{date_time}',
                    ISSUER_CODE = '{issuer_code}', 
                    ACQUIRER_CODE = '{acquirer_code}', 
                    ACQ_FLG = {acq_flg if acq_flg is not None else 'ACQ_FLG'}, 
                    ISS_FLG = {iss_flg if iss_flg is not None else 'ISS_FLG'}, 
                    ACQ_FLG_DATE = {acq_flg_date if acq_flg_date is not None else 'ACQ_FLG_DATE'}, 
                    ISS_FLG_DATE = {iss_flg_date if iss_flg_date is not None else 'ISS_FLG_DATE'}
                WHERE TRN_REF = '{trn_ref}'
            """
            try:
                execute_query(server, database, username, password, update_query, query_type="UPDATE")
                return "function works"
            except Exception as e:
                logging.error(f"Error updating TRN_REF {trn_ref}: {e}")
        else:
            insert_query = f"""
                INSERT INTO reconciliation 
                    (DATE_TIME, TRN_REF, TRAN_DATE, ISSUER_CODE, ACQUIRER_CODE,EXCEP_FLAG, ACQ_FLG, ISS_FLG, ACQ_FLG_DATE, ISS_FLG_DATE) 
                VALUES 
                    ('{current_datetime}', '{trn_ref}', '{date_time}', '{issuer_code}', '{acquirer_code}','{temp_except_flag}' {acq_flg}, {iss_flg}, {acq_flg_date}, {iss_flg_date})
            """
            try:
                execute_query(server, database, username, password, insert_query, query_type="INSERT")
            except Exception as e:
                logging.error(f"Error inserting TRN_REF {trn_ref}: {e}")
