from logging import Logger, error, warn
import os
import os.path as path
import shutil
import sys
from typing import List, Literal

from deltalake import write_deltalake # type: ignore

from azure.storage.filedatalake import (
    DataLakeServiceClient,
)
from .db_tools import table_to_dataframe
from .onelake_tools import (
    copy_deltatable,
    get_service_client_token_credential
)
logger : Logger | None = None

def upload_table_lakehouse(
    sql_server: str,
    database_name: str,
    source: List[str] | str,
    workspace_name: str,
    lakehouse_name: str,
    storage_account: str | None = None,
    tenant_id : str | None = None,
    client_id : str | None = None,
    client_secret : str | None = None,
    deltalake_mode: Literal['error', 'append', 'overwrite', 'ignore'] = "overwrite",
    target_table: str | None = None,
    service_client : DataLakeServiceClient | None = None,
    temp_table_location: str | None = "output"
):
    """
    Uploads a delta table from SQL Server to a directory in Azure Data Lake Storage.

    Parameters:
        sql_server (str): Address of SQL Server.
        database_name (str): Name of database.
        source (str): Query or name of table (schema required)
        workspace_name (str): Name of Fabric-enabled PowerBI workspace
        lakehouse_name (str): Name of Lakehouse in PowerBI workspace (either lakehouse_name or lakehouse_name.Lakehouse will work)
        storage_account (str, None, optional): Storage account to use. Onelakes typically operate on the same address - can either be just name or full URL
        tenant_id (str, None, optional): Tenant ID if using Token Credentials
        client_id (str, None, optional): Client ID if using Token Credentials
        client_secret (str, None, optional): Client Secret if using Token Credentials
        deltalake_mode (str, ('error', 'append', 'overwrite', 'ignore'), optional): ! this will probably be removed
        target_table (str, None, optional): Target table name to write to on Fabric Lakehouse. Required when passing query, optional when passing a table, and disabled when passing multiple tables. 
        service_client (DataLakeServiceClient, None, optional): Could be passed in if user wanted to authenticate a different way, or use a shared connection
        temp_table_location (str, None, optional): this is where the delta tables will be stored locally. Defaults to "output".
    """

    if isinstance(source,str) and " from " in source.lower() and not target_table: 
        if logger:
            logger.error("If source provided is a query, you MUST pass a target_table.")
        else:
            error("If source provided is a query, you MUST pass a target_table.")
        raise Exception("No target_table provided with query.")
    if not isinstance(source, list):
        if "," in source and " from " not in source.lower():
            source = source.split(",")
            
        elif " from " in source.lower():
            source = [source]
        else:
            source = [source]
    if temp_table_location is None:
        temp_table_location = "output"
    if target_table and len(source) > 1 :
        if logger: logger.warn("target_table provided for list of tables, which is not supported.")
        else: warn("target_table for list of tables, which is not supported.")
        if not sys.stdin.isatty():
            sys.exit(1)
        user_input = input("Ignore parameter target_table? (y to continue): ")
        if user_input.lower() != 'y':
            if logger: logger.warn("Exiting.")
            else: warn("Exiting.")
            sys.exit()
        target_table = None
    if service_client is None:
        service_client = get_service_client_token_credential(
            storage_account,
            service_prinicipal_tenant_id=tenant_id,
            service_prinicipal_client_id=client_id,
            service_prinicipal_client_secret=client_secret
        )

    for query_or_table in source:
        query_or_table = query_or_table.lstrip().rstrip()

        df = table_to_dataframe( 
            sql_server,
            database_name,
            query_or_table
        )

        if target_table and len(source) == 1:
            query_or_table = target_table
        if query_or_table and "." in query_or_table:
            query_or_table = query_or_table.replace(".", "_")
        if query_or_table and query_or_table.startswith("dbo_"):
            query_or_table = query_or_table[4:]
        if len(source) == 1 and temp_table_location != "output":
            _temp_table_location = temp_table_location
        else:
            _temp_table_location = f"{path.join(temp_table_location, query_or_table)}".replace('\\', '/')
        if path.exists(_temp_table_location): shutil.rmtree(_temp_table_location)

        write_deltalake(_temp_table_location, df, mode=deltalake_mode)
        target_tablename = os.path.basename(_temp_table_location)
        print(f"Starting:\t{sql_server}.{database_name}.{query_or_table} => /{workspace_name}/{lakehouse_name}/Tables/{target_tablename}")
        copy_deltatable(service_client, _temp_table_location, lakehouse_name, workspace_name)
        print(f"Finished:\t{sql_server}.{database_name}.{query_or_table} => /{workspace_name}/{lakehouse_name}/Tables/{target_tablename}")

def upload_csv_lakehouse(
    sql_server: str,
    database_name: str,
    source: List[str] | str,
    workspace_name: str,
    lakehouse_name: str,
    storage_account: str | None = None,
    tenant_id : str | None = None,
    client_id : str | None = None,
    client_secret : str | None = None,
    deltalake_mode: Literal['error', 'append', 'overwrite', 'ignore'] = "overwrite",
    target_file: str | None = None,
    service_client : DataLakeServiceClient | None = None,
    temp_csv_location: str | None = "output"
):
    """
    Uploads a delta table from SQL Server to a directory in Azure Data Lake Storage.

    Parameters:
        sql_server (str): Address of SQL Server.
        database_name (str): Name of database.
        source (str): Query or name of table (schema required)
        workspace_name (str): Name of Fabric-enabled PowerBI workspace
        lakehouse_name (str): Name of Lakehouse in PowerBI workspace (either lakehouse_name or lakehouse_name.Lakehouse will work)
        storage_account (str, None, optional): Storage account to use. Onelakes typically operate on the same address - can either be just name or full URL
        tenant_id (str, None, optional): Tenant ID if using Token Credentials
        client_id (str, None, optional): Client ID if using Token Credentials
        client_secret (str, None, optional): Client Secret if using Token Credentials
        deltalake_mode (str, ('error', 'append', 'overwrite', 'ignore'), optional): ! this will probably be removed
        target_table (str, None, optional): Target table name to write to on Fabric Lakehouse. Required when passing query, optional when passing a table, and disabled when passing multiple tables. 
        service_client (DataLakeServiceClient, None, optional): Could be passed in if user wanted to authenticate a different way, or use a shared connection
        temp_table_location (str, None, optional): this is where the delta tables will be stored locally. Defaults to "output".
    """

    if isinstance(source,str) and " from " in source.lower() and not target_table: 
        if logger:
            logger.error("If source provided is a query, you MUST pass a target_table.")
        else:
            error("If source provided is a query, you MUST pass a target_table.")
        raise Exception("No target_table provided with query.")
    if not isinstance(source, list):
        if "," in source and " from " not in source.lower():
            source = source.split(",")
            
        elif " from " in source.lower():
            source = [source]
        else:
            source = [source]
    if temp_csv_location is None:
        temp_csv_location = "output"
    if target_file and len(source) > 1 :
        if logger: logger.warn("target_table provided for list of tables, which is not supported.")
        else: warn("target_table for list of tables, which is not supported.")
        if not sys.stdin.isatty():
            sys.exit(1)
        user_input = input("Ignore parameter target_table? (y to continue): ")
        if user_input.lower() != 'y':
            if logger: logger.warn("Exiting.")
            else: warn("Exiting.")
            sys.exit()
        target_table = None
    if service_client is None:
        service_client = get_service_client_token_credential(
            storage_account,
            service_prinicipal_tenant_id=tenant_id,
            service_prinicipal_client_id=client_id,
            service_prinicipal_client_secret=client_secret
        )

    for query_or_table in source:
        query_or_table = query_or_table.lstrip().rstrip()

        df = table_to_dataframe( 
            sql_server,
            database_name,
            query_or_table
        )

        if temp_csv_location and len(source) == 1:
            query_or_table = target_file
        if query_or_table and "." in query_or_table:
            query_or_table = query_or_table.replace(".", "_")
        if query_or_table and query_or_table.startswith("dbo_"):
            query_or_table = query_or_table[4:]
        if len(source) == 1 and temp_csv_location != "output":
            _temp_csv_location = temp_csv_location
        else:
            _temp_csv_location = f"{path.join(temp_csv_location, query_or_table)}".replace('\\', '/')
        if path.exists(_temp_csv_location): shutil.rmtree(_temp_csv_location)

        # write_csvfile(_temp_csv_location, df, mode=deltalake_mode)
        target_file = target_file if target_file else query_or_table
        target_tablename = os.path.basename(_temp_csv_location)
        print(f"Starting:\t{sql_server}.{database_name}.{query_or_table} => /{workspace_name}/{lakehouse_name}/Files/{target_tablename}")
        
        print(f"Finished:\t{sql_server}.{database_name}.{query_or_table} => /{workspace_name}/{lakehouse_name}/Tables/{target_tablename}")

def create_local_directory_if_not_exists(directory: str):
    """
    Creates a directory if it doesn't already exist.

    Parameters:
        directory (str): The directory to create.
    """
    if not path.exists(directory):
        os.makedirs(directory)
