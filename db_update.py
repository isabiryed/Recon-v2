import pandas as pd
import pyodbc
from db_connect_aws import execute_query

def batch_update(df, Swift_code_up, min_date, max_date, server, database, username, password, execute_query_func):
    # Filter the DataFrame to get the rows that need updating
    to_update = df[
        (df['Recon Status'] == 'Reconciled') & 
        ((df['ISSUER_CODE'] == Swift_code_up) | 
        (df['ACQUIRER_CODE'] == Swift_code_up))
    ]
    
    trn_refs = to_update['Original_ABC Reference'].tolist()
    # Check if there are any transaction references to update
    if len(trn_refs) > 0:
        trn_refs_str = ', '.join([f"'{ref}'" for ref in trn_refs])
        update_query = f'''
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
        '''
        
        # Generate the count query
        count_query = f'''
            SELECT COUNT(*) 
            FROM Transactions
            WHERE (ISSUER_CODE = '{Swift_code_up}' OR ACQUIRER_CODE = '{Swift_code_up}')
                AND CONVERT(DATE, DATE_TIME) BETWEEN '{min_date}' AND '{max_date}'
                AND (ISS_FLG = 1 OR ACQ_FLG = 1)
                AND TRN_REF IN ({trn_refs_str})
        '''
        
        # count_result = execute_query_func(server, database, username, password, count_query, query_type="SELECT")
        # if count_result is not None:
        #     pre_update_count = count_result.iloc[0, 0]
        # else:
        #     print("Failed to fetch pre-update count. Aborting update.")
        #     return "Failed to fetch pre-update count.", 0

        # Execute the update query
        execute_query_func(server, database, username, password, update_query, query_type="UPDATE")
        
        # Fetch the post-update count
        post_update_count_result = execute_query_func(server, database, username, password, count_query, query_type="SELECT")
        post_update_count = post_update_count_result.iloc[0, 0] if post_update_count_result is not None else 0

        # Feedback
        feedback = f"{post_update_count} rows were updated."

        return feedback, post_update_count
    
    else:
        return "No transaction references to update.", 0
        


            

        # Generate a single SQL update statement for all TRN_REF values
        