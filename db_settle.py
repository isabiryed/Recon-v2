from db_connect import execute_query
import pandas as pd
import logging

def select_setle_file(server, database, username, password, batch):
    try:
        # Define the SQL query for selection
        query = f"""
                SELECT DATE_TIME,TRN_REF, BATCH, TXN_TYPE, ISSUER, ACQUIRER, AMOUNT, FEE, ABC_COMMISSION
                FROM Transactions 
                WHERE (RESPONSE_CODE = '0') 
                AND BATCH = '{batch}' 
                AND ISSUER_CODE != '730147'
                AND TXN_TYPE NOT IN ('ACI', 'AGENTFLOATINQ')
                AND REQUEST_TYPE NOT IN ('1420','1421')
                """
        
        # Execute the SQL query and retrieve the results
        cursor = execute_query(server, database, username, password, query)
        
        # If the cursor is None, handle the error
        if cursor is None:
            raise Exception("No cursor returned from query execution.")

        # datafile = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        datafile = pd.DataFrame(cursor)

        return datafile
    except Exception as e:
        logging.error(f"Error fetching data from the database: {str(e)}")
        return None
    







