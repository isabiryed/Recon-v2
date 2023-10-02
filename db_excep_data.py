import pandas as pd
import logging
from db_connect import execute_query
import pyodbc

# Configuring logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def update_exception_flag(df, server, database, username, password,swift_code):

    if df.empty:
        logging.warning("No Exceptions Records to Update.")
        return

    update_count = 0

    for index, row in df.iterrows():
        trn_ref = row['trn_ref']

        if pd.isnull(trn_ref):
            logging.warning(f"Empty Exceptions Trn Reference for {index}.")
            continue

        # Update Query
        update_query = f"""
            UPDATE reconciliation
        SET
            EXCEP_FLAG = CASE WHEN (EXCEP_FLAG IS NULL OR EXCEP_FLAG = 0 OR EXCEP_FLAG != 1)  
            AND (ISSUER_CODE = '{swift_code}' OR ACQUIRER_CODE = '{swift_code}')  
            THEN 'Y' ELSE 'N' END            
            WHERE TRN_REF = '{trn_ref}'
        """

        try:
            execute_query(server, database, username, password, update_query, query_type="UPDATE")
            update_count += 1
        except pyodbc.Error as err:
            logging.error(f"Error updating PK '{trn_ref}': {err}")

    if update_count == 0:
        logging.info("No Exceptions were updated.")

    exceptions_feedback = f"Updated: {update_count}"
    logging.info(exceptions_feedback)

    return exceptions_feedback
