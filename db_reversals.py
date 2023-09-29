from db_connect import execute_query

def select_reversals(server, database, username, password, swift_code_up):
    # SQL query to select distinct reversals
    reversals_select_query = f"""
        SELECT DISTINCT
            A.DATE_TIME, A.TRN_REF, A.TXN_TYPE, A.ISSUER, A.ACQUIRER, A.AMOUNT,
            A.REQUEST_TYPE AS FIRST_REQUEST,
            A.AGENT_CODE,
            CASE WHEN A.TRAN_STATUS_0 IN ('0') THEN 'Successful' ELSE 'Failed' END AS FIRST_LEG_RESP,
            CASE WHEN A.TRAN_STATUS_1 IN ('0') THEN 'Successful' ELSE 'Failed' END AS SECND_LEG_RESP,
            CASE WHEN B.RESPONSE_CODE IN ('0') THEN 'Successful' ELSE 'Failed' END AS REV_STATUS,
            CASE WHEN B.RESPONSE_CODE IN ('0') THEN NULL ELSE DATEDIFF(SECOND, A.DATE_TIME, GETDATE()) END AS ELAPSED_TIME,
            CASE 
                WHEN B.REQUEST_TYPE IN ('1420') THEN 'First Reversal'
                WHEN B.REQUEST_TYPE IN ('1421') THEN 'Repeat Reversal'
                ELSE 'Unknown' 
            END AS REVERSAL_TYPE	   
    
        FROM Transactions A
        LEFT JOIN (
            SELECT REQUEST_TYPE, TRAN_REF_1, TRAN_REF_0, TRAN_STATUS_1, RESPONSE_CODE
            FROM Transactions
            WHERE REQUEST_TYPE IN ('1420', '1421')
        ) B ON A.TRAN_REF_0 = B.TRAN_REF_1
        WHERE 
            A.REQUEST_TYPE IN ('1200') 
            AND (A.AMOUNT <> 0)
            AND A.TRAN_STATUS_1 IS NOT NULL 
            AND A.TRAN_STATUS_0 IS NOT NULL
            AND (
                (A.TRAN_STATUS_0 IN ('0','00') AND A.TRAN_STATUS_1 NOT IN ('null','00','0')) 
                OR 
                (A.TRAN_STATUS_1 IN ('0','00') AND A.TRAN_STATUS_0 NOT IN ('null','00','0'))
            )
            AND A.TXN_TYPE NOT IN ('ACI','AGENTFLOATINQ','BI','MINI')
            AND (ISSUER_CODE = '{swift_code_up}' OR ACQUIRER_CODE = '{swift_code_up}')
        GROUP BY
            A.DATE_TIME, A.TRN_REF, A.TXN_TYPE, A.ISSUER, A.ACQUIRER, A.AMOUNT,
            A.REQUEST_TYPE, B.REQUEST_TYPE, A.TRAN_REF_0, A.TRAN_REF_1,
            A.AGENT_CODE, B.RESPONSE_CODE, A.RESPONSE_CODE, A.TRAN_STATUS_0, A.TRAN_STATUS_1
        ORDER BY A.DATE_TIME, A.TRN_REF DESC;
    """
    
    # Execute the SQL query and retrieve the results
    reversal_results = execute_query(server, database, username, password, reversals_select_query, query_type="SELECT")
    
    return reversal_results
