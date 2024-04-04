
from logging import Logger
import os
import os.path as path
from typing import Literal
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient,
)
from azure.identity import DefaultAzureCredential, ClientSecretCredential
import validators  # type: ignore

# from typing import Dict

logger : Logger | None = None

class DefaultAzureCredentialOptions:
    """Options for configuring the DefaultAzureCredential."""

    # options : Dict[str,bool]= {
    #     "exclude_workload_identity_credential": True,
    #     "exclude_developer_cli_credential": True,
    #     "exclude_cli_credential" : True,
    #     "exclude_environment_credential" : True,
    #     "exclude_managed_identity_credential" : False,
    #     "exclude_powershell_credential" : True,
    #     "exclude_visual_studio_code_credential" : True,
    #     "exclude_interactive_browser_credential" : True,
    # }
    exclude_workload_identity_credential: bool = True
    exclude_developer_cli_credential: bool = True
    exclude_cli_credential: bool = True
    exclude_environment_credential: bool = True
    exclude_managed_identity_credential: bool = False
    exclude_powershell_credential: bool = True
    exclude_visual_studio_code_credential: bool = True
    exclude_interactive_browser_credential: bool = True

    def __init__(self, exclude_managed_identity_credential: bool = False) -> None:
        self.exclude_managed_identity_credential = exclude_managed_identity_credential
        if exclude_managed_identity_credential:
            self.exclude_cli_credential = True


def get_service_client_token_credential(
    account: str | None = None,
    default_azure_credential_options: DefaultAzureCredentialOptions = DefaultAzureCredentialOptions(),
    service_prinicipal_tenant_id: str | None = None,
    service_prinicipal_client_id: str | None = None,
    service_prinicipal_client_secret: str | None = None,
) -> DataLakeServiceClient:
    """
    Creates and returns a DataLakeServiceClient object using the provided account name.

    Parameters:
        account (str): The name of the Azure Data Lake Storage account, or the URL to the account.

    Returns:
        DataLakeServiceClient: The DataLakeServiceClient object.

    """
    if not account:
        account = "onelake"
    account_url: str = (
        account
        if account.startswith("https://")
        else f"https://{account}.dfs.fabric.microsoft.com"
    )
    if service_prinicipal_client_id is not None and service_prinicipal_tenant_id is not None and service_prinicipal_client_secret is not None:
        if logger: logger.info("Using token credentials.")
        token_credential = ClientSecretCredential(service_prinicipal_tenant_id, service_prinicipal_client_id, service_prinicipal_client_secret)
    else:
        if logger: logger.info("Using default azure credentials.")
        token_credential = DefaultAzureCredential(
            **default_azure_credential_options.__dict__,

        )

    service_client = DataLakeServiceClient(account_url, credential=token_credential)
    
    if logger: logger.info(f"Created DataLakeService client ({account_url=})")

    return service_client


def get_file_system(
    service_client: DataLakeServiceClient, file_system_name: str
) -> FileSystemClient:
    """
    Creates a new file system in the Azure Data Lake Storage account.

    Parameters:
        service_client (DataLakeServiceClient): The DataLakeServiceClient object used.
        file_system_name (str): The name of the file system to create.

    Returns:
        FileSystemClient: The FileSystemClient object representing the newly created file system.
    """
    file_system_client = service_client.get_file_system_client(file_system=file_system_name)  # type: ignore

    return file_system_client


def create_directory_if_not_exists(
    file_system_client: FileSystemClient, lakehouse_name: str, directory_name: str
) -> DataLakeDirectoryClient:
    """
    Creates a directory in the specified file system if it doesn't already exist.

    Args:
        file_system_client (FileSystemClient): The file system client.
        directory_name (str): The name of the directory to create.

    Returns:
        DataLakeDirectoryClient: The client for the created directory.
    """
    create_directory_path = normalize_lakehouse_path(lakehouse_name, directory_name)
    directory_client = file_system_client.get_directory_client(create_directory_path)  # type: ignore

    if directory_client.exists():  # type: ignore
        return directory_client

    return file_system_client.create_directory(create_directory_path)  # type: ignore


def get_directory(
    file_system_client: FileSystemClient, directory_name: str
) -> DataLakeDirectoryClient:
    """
    Creates a directory in the specified file system if it doesn't already exist.

    Args:
        file_system_client (FileSystemClient): The file system client.
        directory_name (str): The name of the directory to create.

    Returns:
        DataLakeDirectoryClient: The client for the created directory.
    """
    directory_client = file_system_client.get_directory_client(directory_name)  # type: ignore

    return directory_client


def upload_file_to_directory(
    directory_client: DataLakeDirectoryClient, local_path: str, file_name: str
):
    """
    Uploads a file to a directory in Azure Data Lake Storage.

    Parameters:
        directory_client (DataLakeDirectoryClient): The client for the target directory.
        local_path (str): The local path of the file to upload.
        file_name (str): The name of the file in the target directory.

    Returns:
        None
    """
    file_client = directory_client.get_file_client(file_name)

    with open(file=local_path, mode="rb") as data:
        data = file_client.upload_data(data, overwrite=True)  # type: ignore


def check_if_file_exists(
    service_client: DataLakeServiceClient,
    workspace_name: str,
    lakehouse_name: str,
    file_path: str,
) -> bool:
    """
    Checks if a file exists in a directory in Azure Data Lake Storage.

    Parameters:
        service_client (DataLakeServiceClient): The DataLakeServiceClient object used.
        workspace_name (str): The name of the workspace.
        lakehouse_name (str): The name of the lakehouse.
        file_path (str): The path of the file to check.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    file_system_client = get_file_system(service_client, workspace_name)  # type: ignore
    delete_file_path = normalize_lakehouse_path(lakehouse_name, file_path)
    directory_path = path.dirname(delete_file_path)
    file_name = path.basename(file_path)
    directory_client = service_client.get_directory_client(file_system_client, directory_path)  # type: ignore

    return __check_if_file_exists(directory_client, file_name)


def __check_if_file_exists(
    directory_client: DataLakeDirectoryClient, file_name: str
) -> bool:
    """
    Checks if a file exists in a directory in Azure Data Lake Storage.

    Parameters:
        directory_client (DataLakeDirectoryClient): The client for the target directory.
        file_name (str): The name of the file in the target directory.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    file_client = directory_client.get_file_client(file_name)

    return file_client.exists()


def delete_file(
    file_system_client: FileSystemClient,
    lakehouse_name: str,
    file_path: str,
):
    """
    Deletes a file in a directory in Azure Data Lake Storage.

    Parameters:
        service_client (DataLakeServiceClient): The DataLakeServiceClient object used.
        file_path (str): The path of the file to delete.
    """
    delete_file_path = normalize_lakehouse_path(lakehouse_name, file_path)

    file_system_client.delete_file(delete_file_path)  # type: ignore
def delete_table(
    service_client: DataLakeServiceClient,
    workspace_name: str,
    lakehouse_name: str,
    table_name: str,
):
    """
    Deletes a directory in Azure Data Lake Storage.

    Parameters:
        service_client (DataLakeServiceClient): The DataLakeServiceClient object used.
        directory_path (str): The path of the directory to delete.
    """
    # file_system_client = get_file_system(service_client, workspace_name)  # type: ignore
    delete_table_path = normalize_lakehouse_path(lakehouse_name, table_name, type="Tables")
    file_system_client = service_client.get_file_system_client(workspace_name)  # type: ignore
    directory_client = file_system_client.get_directory_client(delete_table_path)  # type: ignore
    if directory_client.exists():  # type: ignore
        if logger: logger.debug(f"Deleting existing table on Lakehouse: {delete_table_path}")
        file_system_client.delete_directory(delete_table_path)  # type: ignore
    directory_client.close()  # type: ignore

def delete_directory(
    service_client: DataLakeServiceClient,
    workspace_name: str,
    lakehouse_name: str,
    directory_path: str,
):
    """
    Deletes a directory in Azure Data Lake Storage.

    Parameters:
        service_client (DataLakeServiceClient): The DataLakeServiceClient object used.
        directory_path (str): The path of the directory to delete.
    """
    # file_system_client = get_file_system(service_client, workspace_name)  # type: ignore
    delete_directory_path = normalize_lakehouse_path(lakehouse_name, directory_path)
    file_system_client = service_client.get_file_system_client(workspace_name)  # type: ignore
    directory_client = file_system_client.get_directory_client(delete_directory_path)  # type: ignore
    if directory_client.exists():  # type: ignore
        if logger: logger.debug(f"Deleting existing directory on Lakehouse: {delete_directory_path}")

        file_system_client.delete_directory(delete_directory_path)  # type: ignore
    directory_client.close()  # type: ignore


def count_files_in_directory(
    service_client: DataLakeServiceClient,
    workspace_name: str,
    lakehouse_name: str,
    directory_path: str,
) -> int:
    """
    Counts the number of files in a directory in Azure Data Lake Storage.

    Parameters:
        service_client (DataLakeServiceClient): The DataLakeServiceClient object used.
        workspace_name (str): The name of the workspace.
        lakehouse_name (str): The name of the lakehouse.
        directory_path (str): The path of the directory to check.

    Returns:
        int: The number of files in the directory.
    """
    count_directory_path = normalize_lakehouse_path(lakehouse_name, directory_path)
    file_system_client = service_client.get_file_system_client(workspace_name)  # type: ignore

    return len(list(filter(lambda path: not path.is_directory, file_system_client.get_paths(path=count_directory_path))))  # type: ignore


def normalize_lakehouse_path(
    lakehouse_name: str, sink_directory: str, workspace_name: str | None = None, type: Literal["Files", "Tables"] = "Files"
) -> str:
    """
    Normalizes the sink path for the Azure Data Lake Storage.

    Parameters:
        lakehouse_name (str): The name of the lakehouse.
        sink_directory (str): The name of the sink directory.

    Returns:
        str: The normalized sink path.
    """
    if not lakehouse_name.endswith(".Lakehouse"):
        lakehouse_name = f"{lakehouse_name}.Lakehouse"
    if sink_directory.startswith("/"):
        sink_directory = sink_directory[len("/") :]
    if workspace_name:
        if workspace_name.startswith("/"):
            workspace_name = workspace_name[len("/") :]
        if workspace_name.endswith("/"):
            workspace_name = workspace_name[: len(workspace_name) - 1]
        lakehouse_name = f"{workspace_name}/{lakehouse_name}"
    if sink_directory.startswith(f"{type}/"):
        sink_directory = sink_directory[len(f"{type}/") :]
    return f"{lakehouse_name}/{type}/{sink_directory}"

def copy_deltatable(
    service_client: DataLakeServiceClient,
    local_table_path: str,
    lakehouse_name: str,
    workspace_name: str
):

    file_system_client = service_client.get_file_system_client(workspace_name) # type: ignore


    target_directory = os.path.basename(local_table_path)

    for dirpath, _dirnames, filenames in os.walk(local_table_path):

        for filename in filenames:
            local_file_path = os.path.join(dirpath, filename).replace("\\", "/")

            delete_table(service_client,workspace_name, lakehouse_name,target_directory)
            lakehouse_path = normalize_lakehouse_path(lakehouse_name, target_directory, type= "Tables")
            file_path = __parquet_filename_to_snappy(os.path.relpath(local_file_path, local_table_path).replace("\\", "/"))
            
            data_lake_file_path = os.path.join(lakehouse_path, file_path).replace("\\", "/")
            directory_client = get_directory(file_system_client, lakehouse_path)
            if logger: logger.debug(f"Copying {local_file_path=} to {data_lake_file_path=}")
            file_client = directory_client.get_file_client(file_path)

            with open(local_file_path, 'rb') as local_file:
                file_client.upload_data(local_file, overwrite=True) # type: ignore

def __parquet_filename_to_snappy(parquet_file_name :str) -> str:
    return parquet_file_name

        