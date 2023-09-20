
from db_connect_aws import execute_query

def select_exceptions(server, database, username, password,Swift_code_up):
    # Define the SQL query for selection
    select_query = f"""
        SELECT DISTINCT DATE_TIME,TRN_REF,TXN_TYPE,ISSUER,ACQUIRER,AMOUNT,TRANSACTION_STATUS,
                    TRAN_STATUS_0 AS LEG1_STATUS,TRAN_STATUS_1 AS LEG2_STATUS
                    FROM Transactions WHERE (ISSUER_CODE = '{Swift_code_up}' OR ACQUIRER_CODE = '{Swift_code_up}')
                    AND EXCEP_FLG = 'Y'     """
    
    # Execute the SQL query and retrieve the results
    excep_results = execute_query(server, database, username, password, select_query, query_type="SELECT")
    
    return excep_results