import os
import os.path as path
from typing import List, Literal

from deltalake import write_deltalake # type: ignore

from .db_tools import execute_bsp_csv, table_to_dataframe
from .onelake_tools import (
    copy_deltatable,
    get_service_client_token_credential,
    create_directory_if_not_exists,
    upload_file_to_directory,
)
from azure.storage.filedatalake import (
    DataLakeServiceClient,
)
def upload_table_csv_lakehouse(
    sql_server: str,
    database_name: str,
    schema_name: str,
    table_name: List[str] | str,
    sink_account_name: str,
    sink_workspace_name: str,
    sink_lakehouse_name: str,
    sink_directory: str,
    csv_store_directory: str | None = None,
    csv_file_suffix: str | None = None,
    service_client : DataLakeServiceClient | None = None
):
    """
    Uploads a table (as csv) from SQL Server to a directory in Azure Data Lake Storage.

    Parameters:
        sql_server (str): The name of the SQL Server.
        database_name (str): The name of the database.
        schema_name (str): The name of the schema.
        table_name (str): The name of the table.
        sink_account_name (str): The name of the sink storage account.
        sink_workspace_name (str): The name of the sink workspace.
        sink_lakehouse_name (str): The name of the sink lakehouse.
        sink_directory (str): The name of the sink directory.
        csv_store_directory (str, optional): The local directory to store the CSV file. Defaults to "output".
        csv_file_suffix (str, optional): The suffix to add to the CSV file name. Defaults to None.
    """
    if not isinstance(table_name, list):
        table_name = [table_name]

    if csv_store_directory is None:
        csv_store_directory = "output"

    create_local_directory_if_not_exists(csv_store_directory)
    if service_client is None:
        service_client = get_service_client_token_credential(sink_account_name)


    file_system_client = service_client.get_file_system_client(sink_workspace_name) # type: ignore


    sink_directory_client = create_directory_if_not_exists(
        file_system_client, sink_lakehouse_name, sink_directory
    )

    for table in table_name:
        csv_file_name: str = table.lstrip("[").rstrip("]")

        if csv_file_suffix is not None:
            csv_file_name = f"{csv_file_suffix}{table}"

        output_csv_path = path.join(
            csv_store_directory, f"{csv_file_name}.csv"
        ).replace("\\", "/")

        execute_bsp_csv(sql_server, database_name, schema_name, table, output_csv_path)

        upload_file_to_directory(
            sink_directory_client, output_csv_path, path.basename(output_csv_path)
        )

def upload_table_lakehouse(
    sql_server: str,
    database_name: str,
    schema_name: str,
    table_name: List[str] | str,
    storage_account: str,
    workspace_name: str,
    lakehouse_name: str,
    tenant_id : str | None = None,
    client_id : str | None = None,
    client_secret : str | None = None,
    deltalake_mode: Literal['error', 'append', 'overwrite', 'ignore'] = "overwrite",
    service_client : DataLakeServiceClient | None = None,
    temp_table_location: str | None = None,
):
    """
    Uploads a delta table from SQL Server to a directory in Azure Data Lake Storage.

    Parameters:
        sql_server (str): The name of the SQL Server.
        database_name (str): The name of the database.
        schema_name (str): The name of the schema.
        table_name (str): The name of the table.
        sink_account_name (str): The name of the sink storage account.
        sink_workspace_name (str): The name of the sink workspace.
        sink_lakehouse_name (str): The name of the sink lakehouse.
        sink_directory (str): The name of the sink directory.
        csv_store_directory (str, optional): The local directory to store the CSV file. Defaults to "output".
        csv_file_suffix (str, optional): The suffix to add to the CSV file name. Defaults to None.
    """
    
    if not isinstance(table_name, list):
        if "," in table_name:
            table_name = table_name.split(",")
        else:
            table_name = [table_name]
    if temp_table_location is None:
        temp_table_location = "output"
    
    delta_tables : List[str] = []
    for table in table_name:
        if len(table_name) == 1 and temp_table_location == "output":
            _temp_table_location = f"{path.join(temp_table_location, table)}".replace('\\', '/')
        elif len(table_name) == 1:
            _temp_table_location = temp_table_location
        else:
            _temp_table_location = f"{path.join(temp_table_location, table)}".replace('\\', '/')
        df = table_to_dataframe(
            sql_server,
            database_name,
            schema_name,
            table
        )

        write_deltalake(_temp_table_location, df, mode=deltalake_mode)
        delta_tables.append(_temp_table_location)
    
    if service_client is None:
        if client_id is not None:
            
            service_client = get_service_client_token_credential(
                storage_account,
                service_prinicipal_tenant_id=tenant_id,
                service_prinicipal_client_id=client_id,
                service_prinicipal_client_secret=client_secret
            )
        else:
            service_client = get_service_client_token_credential(storage_account)
    for delta_table in delta_tables:
        copy_deltatable(service_client, delta_table, lakehouse_name, workspace_name)




def create_local_directory_if_not_exists(directory: str):
    """
    Creates a directory if it doesn't already exist.

    Parameters:
        directory (str): The directory to create.
    """
    if not path.exists(directory):
        os.makedirs(directory)
