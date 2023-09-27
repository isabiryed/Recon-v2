import pandas as pd
import logging
from datetime import datetime
from db_connect import execute_query
import pyodbc

# Configuring logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def update_reconciliation(df, server, database, username, password, swift_code):

    if df.empty:
        logging.warning("No Records to Update.")
        return

    update_count = 0
    insert_count = 0

    for index, row in df.iterrows():
        date_time = row['DATE_TIME']
        batch = row['BATCH']
        trn_ref = row['TRN_REF2']
        issuer_code = row['ISSUER_CODE']
        acquirer_code = row['ACQUIRER_CODE']

        if pd.isnull(trn_ref):
            logging.warning(f"Empty Trn Reference for {index}.")
            continue

        select_query = f"SELECT * FROM reconciliation WHERE TRN_REF = '{trn_ref}'"
        existing_data = execute_query(server, database, username, password, select_query, query_type="SELECT")

        # Update Query
        update_query = f"""
            UPDATE reconciliation
        SET
            ISS_FLG = CASE WHEN (ISS_FLG IS NULL OR ISS_FLG = 0 OR ISS_FLG != 1)  AND ISSUER_CODE = '{swift_code}' THEN 1 ELSE ISS_FLG END,
            ACQ_FLG = CASE WHEN (ACQ_FLG IS NULL OR ACQ_FLG = 0 OR ACQ_FLG != 1) AND ACQUIRER_CODE = '{swift_code}' THEN 1 ELSE ACQ_FLG END,
            ISS_FLG_DATE = CASE WHEN (ISS_FLG IS NULL OR ISS_FLG = 0 OR ISS_FLG != 1) AND ISSUER_CODE = '{swift_code}' THEN GETDATE() ELSE ISS_FLG_DATE END,
            ACQ_FLG_DATE = CASE WHEN (ACQ_FLG IS NULL OR ACQ_FLG = 0 OR ACQ_FLG != 1) AND ACQUIRER_CODE = '{swift_code}' THEN GETDATE() ELSE ACQ_FLG_DATE END
            WHERE TRN_REF = '{trn_ref}'                
        """

        if existing_data.empty:
            # If not existing, insert and then update
            insert_query = f"""
                INSERT INTO reconciliation 
                    (DATE_TIME, TRAN_DATE, TRN_REF, BATCH, ACQUIRER_CODE, ISSUER_CODE)
                VALUES 
                    (GETDATE(),
                     '{date_time}',
                     '{trn_ref}', 
                     '{batch}', 
                     '{acquirer_code}',
                     '{issuer_code}')
            """
            try:
                execute_query(server, database, username, password, insert_query, query_type="INSERT")
                insert_count += 1
                # Immediate update after insert
                execute_query(server, database, username, password, update_query, query_type="UPDATE")
                # update_count += 1
            except pyodbc.Error as err:
                logging.error(f"Error processing PK '{trn_ref}': {err}")
        else:
            # If already existing, just update
            try:
                execute_query(server, database, username, password, update_query, query_type="UPDATE")
                update_count += 1
            except pyodbc.Error as err:
                logging.error(f"Error updating PK '{trn_ref}': {err}")

    if update_count == 0:
        logging.info("No new records were updated.")
    if insert_count == 0:
        logging.info("No new records were inserted.")

    feedback = f"Updated: {update_count}, Inserted: {insert_count}"
    logging.info(feedback)

    return feedback