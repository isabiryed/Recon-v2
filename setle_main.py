import pandas as pd
import re

from db_settle_recon import select_setle_file

server = 'abcbusinessintelligence.database.windows.net'
database = 'BusinessIntelligence'
username = "isabiryed"
password = "Vp85FRFXYf2KBr@"

batch = 2349


def combine_transactions(df: pd.DataFrame, acquirer_col: str = 'Payer', issuer_col: str = 'Beneficiary', 
                         amount_col: str = 'Tran Amount', type_col: str = 'Tran Type') -> pd.DataFrame:
    """
    Combine transactions based on certain conditions.

    :param df: Input DataFrame.
    :param acquirer_col: Column name for Acquirer.
    :param issuer_col: Column name for Issuer.
    :param amount_col: Column name for Transaction Amount.
    :param type_col: Column name for Transaction Type.
    :return: New DataFrame with combined transaction amounts.
    """
    combined_dict = {}

    for index, row in df.iterrows():
        acquirer = row[acquirer_col]
        issuer = row[issuer_col]
        tran_amount = row[amount_col]
        tran_type = row[type_col]
        key = (acquirer, issuer)
    
        if acquirer != issuer and tran_type not in ["CLF", "CWD"]:
            combined_dict[key] = combined_dict.get(key, 0) + tran_amount

        if acquirer != issuer and tran_type in ["CLF", "CWD"]:
            combined_dict[key] = combined_dict.get(key, 0) + tran_amount

        # where issuer & acquirer = TROP BANK AND service = NWSC , UMEME settle them with BOA
        if acquirer == "TROAUGKA" and issuer == "TROAUGKA" and tran_type in ["NWSC", "UMEME"]:
            tro_key = ("TROAUGKA", "AFRIUGKA")
            combined_dict[tro_key] = combined_dict.get(tro_key, 0) + tran_amount

    # Convert combined_dict to DataFrame
    combined_result = pd.DataFrame(combined_dict.items(), columns=["Key", amount_col])
    # Split the "Key" column into Acquirer and Issuer columns
    combined_result[[acquirer_col, issuer_col]] = pd.DataFrame(combined_result["Key"].tolist(), index=combined_result.index)
    
    # Drop the "Key" column
    combined_result = combined_result.drop(columns=["Key"])
    
    return combined_result

def add_payer_beneficiary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds 'Payer' and 'Beneficiary' columns to the DataFrame.

    :param df: Input DataFrame.
    :return: DataFrame with 'Payer' and 'Beneficiary' columns added.
    """
    df['Payer'] = df['ACQUIRER']
    df['Beneficiary'] = df['ISSUER']
    return df

def pre_processing_amt(df):
    # Helper function
    def clean_amount(value):
        try:
            # Convert the value to a float, round to nearest integer
            return round(float(value))  # round the value and return as integer
        except:
            return value  # Return the original value if conversion fails
    
    # Cleaning logic
    for column in ['AMOUNT', 'FEE', 'ABC_COMMISSION']:  # only these columns
        df[column] = df[column].apply(clean_amount)
    
    return df

def convert_batch_to_int(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts the 'BATCH' column to numeric, rounds it to the nearest integer, and fills NaN with 0.

    :param df: DataFrame containing the 'BATCH' column to convert.
    :return: DataFrame with the 'BATCH' column converted.
    """
    # Check data type and convert 'BATCH' column to numeric
    df['BATCH'] = pd.to_numeric(df['BATCH'], errors='coerce')
    # Apply the round method
    df['BATCH'] = df['BATCH'].round(0).fillna(0).astype(int)
    
    return df

def main(batch):
    # Execute the SQL query
    datadump = select_setle_file(server, database, username, password, batch)
     
    # Check if datadump is not None
    if datadump is not None and not datadump.empty:         
        datadump = convert_batch_to_int(datadump)
        datadump = pre_processing_amt(datadump)
        datadump = add_payer_beneficiary(datadump)
        combined_result = combine_transactions(datadump, acquirer_col='Payer', issuer_col='Beneficiary', amount_col='AMOUNT', type_col='TXN_TYPE')
        # print(combined_result)
    else:
        print("No records for processing found.")

# # Call the main function
main(batch)


