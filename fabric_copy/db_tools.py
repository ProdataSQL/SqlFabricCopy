""" Module with functions for working with SQL database. """
import subprocess
from typing import List
import os.path as path
import os
import pyarrow as pa # type: ignore
import pandas as pd
import pyodbc
def execute_bsp_csv(
        sql_server: str,
        database_name: str,
        schema_name: str,
        table_names: str|List[str],
        output_csv_path: str,
        seperator: str = ","
):
    """
    Uploads a table to a directory in Azure Data Lake Storage.
    
    Throws:
        subprocess.CalledProcessError: If the bcp command fails.
    
    Parameters:
        sql_server (str): The name of the SQL Server.
        database_name (str): The name of the database.
        schema_name (str): The name of the schema.
        table_name (str): The name of the table.
        output_csv_path (str): The path of the output CSV file.
        seperator (str, optional): The field separator for the CSV file. Defaults to ",".
    """
    if not isinstance(table_names,list):
        table_names = [table_names]

    for table in table_names:
        if not output_csv_path.endswith(".csv"):
            fp = path.join(output_csv_path, table.lstrip('[').rstrip(']')).replace('\\', '/')
            file_out = f"{fp}.csv"
        else:
            file_out = output_csv_path
            
        directory = path.dirname(file_out)
        if not path.exists(directory):
            os.makedirs(directory)

        arguments : List[str] = [
            f"{database_name}.{schema_name}.{table}", # table to export
            "out",                                         # export direction
            file_out,                               # output file
            "-c",                                          # character data
            f"-t\"{seperator}\"",                          # field terminator (default: ,)
            "-T",                                          # use trusted connection
            f"-S {sql_server}"                             # server name
        ]

        bcp_run = subprocess.run(["bcp"] + arguments, stdin=subprocess.DEVNULL, check=True)

        bcp_run.check_returncode()

def table_to_dataframe(
        sql_server: str,
        database_name: str,
        schema_name: str,
        table_name : str,
) -> pd.DataFrame: # type: ignore

    cnxn = pyodbc.connect(driver='{SQL Server}',
                          server=sql_server,
                          database=database_name,
                          trusted_connection='yes'
    )
    query = f"SELECT * FROM {schema_name}.{table_name}"
    return pd.read_sql(query, cnxn, dtype_backend="pyarrow") # type: ignore