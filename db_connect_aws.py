import pyodbc
import pandas as pd

def execute_query(server_name, database_name, username, password, query, query_type="SELECT"):
    conn = None  # Initialize conn to None
    try:
        # Define the connection string
        connection_string = f"Driver={{SQL Server}};Server={server_name};Database={database_name};UID={username};PWD={password};TrustServerCertificate=yes;"

        # Connect to the database
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Execute the query based on the query type
        if query_type == "SELECT":
            cursor.execute(query)
            # Fetch data directly into a DataFrame with column names
            df = pd.DataFrame.from_records(cursor.fetchall(), columns=[column[0] for column in cursor.description])
            return df
        elif query_type in ["UPDATE", "INSERT"]:
            cursor.execute(query)
            conn.commit()  # Commit the changes to the database
            return None  # For update and Insert queries, return None
        else:
            raise ValueError("Invalid query type. Supported types are 'SELECT','UPDATE'and 'INSERT'.")

    except Exception as e:
        print(f"Error: {str(e)}")
        return None  # Return None in case of an error
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Example usage for SELECT query:
    server = 'businessintelligence.cqo7jevz1qxm.eu-north-1.rds.amazonaws.com,1433'
    database = 'BusinessIntelligence'
    username = "isabiryed"
    password = "Tech247w247"

    # Test the connection
    test_query = "SELECT TOP 2 DATE_TIME, TRN_REF, TXN_TYPE FROM Transactions"  # A simple query to test the connection
    result_df = execute_query(server, database, username, password, test_query)

    if result_df is not None:
        print("Connection successful.")
        print(result_df)  # Print the result of the test query
    else:
        print("Connection failed.")
