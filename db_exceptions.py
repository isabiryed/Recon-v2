
from db_connect import execute_query

def select_exceptions(server, database, username, password,Swift_code_up):
    # Define the SQL query for selection
    excep_select_query = f"""
        SELECT DISTINCT DATE_TIME, TRAN_DATE, TRN_REF, BATCH, ACQUIRER_CODE, ISSUER_CODE, EXCEP_FLAG,
            CASE WHEN ACQ_FLG = 1 OR ISS_FLG =1 THEN 'Partly Receonciled' WHEN ACQ_FLG = 1 AND ISS_FLG =1 THEN 'Fully Reconciled' END AS RECON_STATUS

            FROM reconciliation 
            WHERE EXCEP_FLAG IS NOT NULL
            AND (ISSUER_CODE = '{Swift_code_up}' OR ACQUIRER_CODE = '{Swift_code_up}')  """
    
    # Execute the SQL query and retrieve the results
    excep_results = execute_query(server, database, username, password, excep_select_query, query_type="SELECT")
    
    return excep_results