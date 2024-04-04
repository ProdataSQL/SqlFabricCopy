"""
    Test cases for sql_fabric_copy module.
"""

import unittest
import configparser
import os
import os.path as path
import shutil
from deltalake.writer import write_deltalake # type: ignore
from sql_fabric_copy.db_tools import execute_bsp_csv, table_to_dataframe # type: ignore
from sql_fabric_copy.sql_fabric_copy_helper import upload_csv_lakehouse, upload_table_lakehouse
from sql_fabric_copy.onelake_tools import (
    DefaultAzureCredentialOptions,
    count_files_in_directory,
    delete_directory,
    get_service_client_token_credential,
)

class TestFabricCopy(unittest.TestCase):
    """
    Test cases for sql_fabric_copy module.
    """

    def setUp(self) -> None:
        super().setUp()
        config = configparser.ConfigParser()
        config.read("./test_config.ini")
        self.sql_server = config.get("TestValues", "sql_server", fallback="localhost")
        self.database_name = (
            config.get("TestValues", "database_name", fallback="AdventureWorksDW")
        )
        self.onelake_account_name = (
            config.get("TestValues", "onelake_account_name", fallback="onelake")
        )
        self.workspace_name = (
            config.get("TestValues", "workspace_name", fallback="FabricDW [Dev]")
        )
        exclude_managed_identity_credential = config.getboolean(
            "TestValues", "exclude_managed_identity_credential", fallback=True
        )
        self.service_client = get_service_client_token_credential(
            self.onelake_account_name,
            DefaultAzureCredentialOptions(exclude_managed_identity_credential=exclude_managed_identity_credential),
            service_prinicipal_tenant_id= config.get("TestValues", "tenant_id", fallback=None),
            service_prinicipal_client_id= config.get("TestValues", "client_id", fallback=None),
            service_prinicipal_client_secret= config.get("TestValues", "client_secret", fallback=None),
        )
    
    def test_delete_directory(self):
        """
        Test case for the delete_directory function.
        """
        sink_lakehouse_name = "FabricLH"
        sink_directory = "temp/dwa0export"

        delete_directory(
            self.service_client,
            self.workspace_name,
            sink_lakehouse_name,
            sink_directory,
        )
    def test_count_directory(self):
        """
        Test case for the delete_directory function.
        """
        sink_lakehouse_name = "FabricLH"
        sink_directory = "unittest/AdventureWorks/erp"
        expected_number_file = 9
        assert count_files_in_directory(
            self.service_client,
            self.workspace_name,
            sink_lakehouse_name,
            sink_directory,
        ) == expected_number_file
    def test_count_directory_managed_identity(self):
        """
        Test case for the delete_directory function.
        """
        sink_lakehouse_name = "FabricLH"
        sink_directory = "unittest/AdventureWorks/erp"
        expected_number_file = 9
        self.service_client = get_service_client_token_credential(
            self.onelake_account_name,
            DefaultAzureCredentialOptions(exclude_managed_identity_credential=False)
        )
        assert count_files_in_directory(
            self.service_client,
            self.workspace_name,
            sink_lakehouse_name,
            sink_directory,
        ) == expected_number_file
    def test_count_directory_cli_cred(self):
        """
        Test case for the delete_directory function.
        """
        sink_lakehouse_name = "FabricLH"
        sink_directory = "unittest/AdventureWorks/erp"
        expected_number_file = 9
        self.service_client = get_service_client_token_credential(
            self.onelake_account_name,
            DefaultAzureCredentialOptions(exclude_managed_identity_credential=True)
        )
        assert count_files_in_directory(
            self.service_client,
            self.workspace_name,
            sink_lakehouse_name,
            sink_directory,
        ) == expected_number_file
    def test_bcp_copy_single(self):
        """
        Test case for the execute_bsp_csv function for a single table
        """
        schema_name = "dbo"
        table = "Account"
        output_csv_path = "output"
        delete_directory_if_exists(output_csv_path)
        execute_bsp_csv(self.sql_server, self.database_name, schema_name, table, output_csv_path)
    
        assert count_files(output_csv_path) == 1

    def test_csv_to_onelake(self):
        arguments = {
            'storage_account': None,
            'sql_server': self.sql_server,
            'database_name': self.database_name,
            'source': 'aw.DimCurrency',
            'workspace_name': self.workspace_name,
            'lakehouse_name': "FabricLH",
            'tenant_id': None,
            'client_id': None,
            'client_secret': None
        }
        upload_csv_lakehouse(
            **arguments # type: ignore
        )
    def test_bcp_copy_multiple(self):
        """
        Test case for the execute_bsp_csv function for a single table
        """
        schema_name = "dbo"
        tables = ["Account", "FactWatermarks"]
        output_csv_path = "output"
        delete_directory_if_exists(output_csv_path)
        execute_bsp_csv(self.sql_server, self.database_name, schema_name, tables, output_csv_path)
    
        assert count_files(output_csv_path) == len(tables)
    def test_table_to_dataframe(self):
        table = "dbo.Account"
        df = table_to_dataframe( # type: ignore
            self.sql_server,
            self.database_name,
            table
        )

        output_delta_path = "output/Account"

        write_deltalake(output_delta_path, df, mode="overwrite") # type: ignore
    def test_table_to_onelake(self):
        arguments = {
            'storage_account': None,
            'sql_server': self.sql_server,
            'database_name': self.database_name,
            'source': 'aw.DimCurrency',
            'workspace_name': self.workspace_name,
            'lakehouse_name': "FabricLH",
            'target_table': None,
            'tenant_id': None,
            'client_id': None,
            'client_secret': None
        }

        upload_table_lakehouse(
            **arguments # type: ignore
        )
    
    def test_table_query_to_onelake(self):
        arguments = {
            'storage_account': None,
            'sql_server': 'localhost',
            'database_name': 'AdventureWorksDW',
            'source': 'SELECT * FROM aw.DimAccount',
            'workspace_name': 'FabricDW [Dev]',
            'lakehouse_name': 'FabricLH', 
            'target_table': 'DimAccount', 
            'tenant_id': None, 
            'client_id': None, 
            'client_secret': None
            }
        upload_table_lakehouse(
            **arguments # type: ignore
        )
def delete_directory_if_exists(directory :str):
    if path.exists(directory): shutil.rmtree(directory)
def count_files(directory :str ) -> int:
    return len([name for name in os.listdir(directory) if os.path.isfile(os.path.join(directory, name))])
    
if __name__ == '__main__':
    unittest.main()