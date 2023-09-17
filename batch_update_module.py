import pandas as pd
import pyodbc
from db_connect_aws import execute_query

def batch_update(df, Swift_code_up, min_date, max_date, server, database, username, password, execute_query_func):
    """
    Batch updates the Transactions table based on the provided DataFrame and conditions.
    
    Parameters:
    - df: DataFrame containing the data.
    - Swift_code_up: Swift code to be used in the update conditions.
    - min_date, max_date: Date range for the update conditions.
    - server, database, username, password: Database connection parameters.
    - execute_query_func: Function to execute the SQL queries.
    
    Returns:
    - Feedback message indicating the result of the update.
    """
    
    # Filter the DataFrame to get the rows that need updating
    to_update = df[
        (df['Recon Status'] == 'Reconciled') & 
        ((df['ISSUER_CODE'] == Swift_code_up) | 
        (df['ACQUIRER_CODE'] == Swift_code_up))
    ]

    # List to hold all TRN_REF values for the update
    trn_refs = to_update['Original_ABC Reference'].tolist()

    # Generate a single SQL update statement for all TRN_REF values
    trn_refs_str = ', '.join([f"'{ref}'" for ref in trn_refs])
    update_query = f"""
        UPDATE Transactions
        SET
            ISS_FLG = CASE
                WHEN ISS_FLG != 1 AND ISSUER_CODE = '{Swift_code_up}' THEN 1
                ELSE ISS_FLG
            END,
            ACQ_FLG = CASE
                WHEN ACQ_FLG != 1 AND ACQUIRER_CODE = '{Swift_code_up}' THEN 1
                ELSE ACQ_FLG
            END,
            ISS_FLG_DATE = CASE
                WHEN (ISS_FLG != 1 AND ISSUER_CODE = '{Swift_code_up}') THEN GETDATE()
                ELSE ISS_FLG_DATE
            END,
            ACQ_FLG_DATE = CASE
                WHEN (ACQ_FLG != 1 AND ACQUIRER_CODE = '{Swift_code_up}') THEN GETDATE()
                ELSE ACQ_FLG_DATE
            END
        WHERE
            CONVERT(DATE, DATE_TIME) BETWEEN '{min_date}' AND '{max_date}'
            AND TRN_REF IN ({trn_refs_str})
    """

    # Fetch the count of rows already updated before executing the update
    count_query = f"""
        SELECT COUNT(*) 
        FROM Transactions
        WHERE (ISSUER_CODE = '{Swift_code_up}' OR ACQUIRER_CODE = '{Swift_code_up}')
            AND CONVERT(DATE, DATE_TIME) BETWEEN '{min_date}' AND '{max_date}'
            AND (ISS_FLG = 1 OR ACQ_FLG = 1)
            AND TRN_REF IN ({trn_refs_str})
    """
    pre_update_count = execute_query_func(server, database, username, password, count_query, query_type="SELECT").iloc[0, 0]

    # Execute the update query
    execute_query_func(server, database, username, password, update_query, query_type="UPDATE")
    
    # Connect to the database to get the rowcount of the last update
    connection_string = f"Driver={{SQL Server}};Server={server};Database={database};UID={username};PWD={password};TrustServerCertificate=yes;"
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # Feedback
    post_update_count = cursor.rowcount
    feedback = ""  # Initialize feedback message

    if post_update_count > 0:
        if post_update_count == pre_update_count:
            feedback = "Rows were already updated previously."
        else:
            difference = post_update_count - pre_update_count
            feedback = f"{difference} rows were updated. {pre_update_count} were already updated previously."
    else:
        feedback = "No rows were updated."

    conn.close()  # Close the connection

    return feedback, post_update_count
