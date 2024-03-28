"""
    Test cases for fabric_copy module.
"""

import unittest
import configparser
import os
import os.path as path
import shutil 
from fabric_copy.db_tools import execute_bsp_csv, table_to_dataframe # type: ignore
from fabric_copy.fabric_copy_helper import upload_table_csv_lakehouse, upload_table_lakehouse
from fabric_copy.onelake_tools import (
    DefaultAzureCredentialOptions,
    count_files_in_directory,
    delete_directory,
    get_service_client_token_credential,
)

from deltalake import DeltaTable
from deltalake.writer import write_deltalake # type: ignore
import pandas as pd


class TestFabricCopy(unittest.TestCase):
    """
    Test cases for fabric_copy module.
    """

    def setUp(self) -> None:
        super().setUp()
        config = configparser.ConfigParser()
        config.read("./Python/test_config.ini")
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
    
    def test_upload_table_csv_lakehouse(self):
        """
        Test case for the upload_table_csv_lakehouse function.
        """
        sql_server = self.sql_server
        database_name = self.database_name
        sink_account_name = self.onelake_account_name
        sink_workspace_name = self.workspace_name
        schema_name = "dbo"
        table_name = ["Account"]
        sink_lakehouse_name = "FabricLH"
        sink_directory = "temp/dwa0export"
        csv_store_directory = "output"
        csv_file_suffix = None

        delete_directory(
            self.service_client,
            self.workspace_name,
            sink_lakehouse_name,
            sink_directory,
        )

        upload_table_csv_lakehouse(
            sql_server,
            database_name,
            schema_name,
            table_name,
            sink_account_name,
            sink_workspace_name,
            sink_lakehouse_name,
            sink_directory,
            csv_store_directory,
            csv_file_suffix,
            self.service_client
        )

        assert count_files_in_directory(
            self.service_client,
            self.workspace_name,
            sink_lakehouse_name,
            sink_directory,
        ) == len(table_name)
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
            DefaultAzureCredentialOptions(exclude_managed_identity_credential=False),
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
            DefaultAzureCredentialOptions(exclude_managed_identity_credential=True),
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
    def test_bcp_copy_csv_to_delta(self):
        """
        Test case for the execute_bsp_csv function for a single table
        """
        schema_name = "dbo"
        table = "Account"
        output_csv_path = "output"
        output_delta_path = "output/table"
        delete_directory_if_exists(output_csv_path)
        execute_bsp_csv(self.sql_server, self.database_name, schema_name, table, output_csv_path)
        csv_file = pd.read_csv(path.join(output_csv_path, f"{table}.csv")) # type: ignore
        dt = DeltaTable(output_delta_path)
        write_deltalake(dt, csv_file, mode="overwrite")
        
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
        schema_name = "dbo"
        table = "Account"
        df = table_to_dataframe( # type: ignore
            self.sql_server,
            self.database_name,
            schema_name,
            table
        )

        output_delta_path = "output/Account"

        write_deltalake(output_delta_path, df, mode="overwrite") # type: ignore
    def test_table_to_onelake(self):
        schema_name = "dbo"
        table = "DimDateRange"
        upload_table_lakehouse(
            self.sql_server,
            self.database_name,
            schema_name,
            table,
            self.onelake_account_name,
            self.workspace_name,
            "FabricLH"
        )
def delete_directory_if_exists(directory :str):
    if path.exists(directory): shutil.rmtree(directory)
def count_files(directory :str ) -> int:
    return len([name for name in os.listdir(directory) if os.path.isfile(os.path.join(directory, name))])
    
        
if __name__ == '__main__':
    unittest.main()