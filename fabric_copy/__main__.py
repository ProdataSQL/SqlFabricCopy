""" Uploads a table (as csv) from SQL Server to a directory in Azure Data Lake Storage. """
import argparse
from .fabric_copy_helper import upload_table_lakehouse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Upload a table from SQL Server to Azure Data Lake Storage.')
    parser.add_argument('--storage_account', required= False, type=str, help='Storage account URL')
    parser.add_argument('--sql_server', required= True, type=str, help='SQL Server address')
    parser.add_argument('--database_name', required= True, type=str, help='Database name')
    parser.add_argument('--schema_name', required= True, type=str, help='Schema name')
    parser.add_argument('--table_name', required= True, type=str, help='Table name')
    parser.add_argument('--workspace_name', required= True, type=str, help='Workspace name')
    parser.add_argument('--lakehouse_name', required= True, type=str, help='Lakehouse name')
    parser.add_argument('--tenant_id', required= False, type=str, help='tenant id used for authentiaction')
    parser.add_argument('--client_id', required= False, type=str, help='client id used for authentiaction')
    parser.add_argument('--client_secret', required= False, type=str, help='client secret used for authentiaction')
    args = vars(parser.parse_args())
    if not args["storage_account"]:
        args["storage_account"] = "onelake"
    upload_table_lakehouse(
        **args
    )
