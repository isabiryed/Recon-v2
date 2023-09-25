from db_connect import execute_query


def insert_recon_stats(bankid,userid,reconciledRows, unreconciledRows, exceptionsRows, feedback, 
                       requestedRows, UploadedRows, date_range_str, server, database, username, password):
    # Define the SQL query for insertion
    insert_query = f"""
        INSERT INTO Reconciliation
        (BANK_ID, USER_ID,RECON_RWS, UNRECON_RWS, EXCEP_RWS, FEEDBACK, RQ_RWS, UPLD_RWS, RQ_DATE_RANGE)
        VALUES
        ({bankid},{userid},{reconciledRows}, {unreconciledRows}, {exceptionsRows}, '{feedback}', {requestedRows}, {UploadedRows}, '{date_range_str}')
    """
    
    # Execute the SQL query
    execute_query(server, database, username, password, insert_query, query_type = "INSERT")


def recon_stats_req(server, database, username, password, bank_id):
    # Define the SQL query for selection using an f-string to insert swift_code
    select_query = f"""
        SELECT RQ_RWS, RQ_DATE_RANGE, UPLD_RWS, EXCEP_RWS, RECON_RWS, UNRECON_RWS, FEEDBACK 
        FROM Reconciliation WHERE BANK_ID = '{bank_id}'
    """
    
    # Execute the SQL query and retrieve the results
    recon_results = execute_query(server, database, username, password, select_query, query_type="SELECT")
    
    return recon_results
