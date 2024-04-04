""" Uploads a table (as csv) from SQL Server to a directory in Azure Data Lake Storage. """
import argparse
from logging import Logger, debug, error
import logging
import sys

from . import db_tools
from . import onelake_tools
from . import sql_fabric_copy_helper


from .sql_fabric_copy_helper import upload_table_lakehouse

LOG_LEVELS = [name for name, value in vars(logging).items() if isinstance(value, int) and not name.startswith('_')]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Upload a table from SQL Server to Azure Data Lake Storage.')
    parser.add_argument('--storage_account', required= False, type=str, help='Storage account URL')
    parser.add_argument('--sql_server', required= True, type=str, help='SQL Server address')
    parser.add_argument('--database_name', required= True, type=str, help='Database name')
    parser.add_argument('--source', required= True, type=str, help='Query or Table Name')
    parser.add_argument('--workspace_name', required= True, type=str, help='Workspace name')
    parser.add_argument('--lakehouse_name', required= True, type=str, help='Lakehouse name')
    parser.add_argument('--target_table', required= False, type=str, help='Required if source is a query')
    parser.add_argument('--tenant_id', required= False, type=str, help='tenant id used for authentiaction')
    parser.add_argument('--client_id', required= False, type=str, help='client id used for authentiaction')
    parser.add_argument('--client_secret', required= False, type=str, help='client secret used for authentiaction')
    parser.add_argument('--log_level', required= False, type=str, help='level of logging to enable (LOG_LEVELS)')
    logging.basicConfig(level=logging.WARNING)
    args = vars(parser.parse_args())
    logger : Logger | None = None
    if args["log_level"]:
        level = logging.getLevelName(args['log_level'].upper())
        if not isinstance(level, int):
            error(f"Log level \"{level}\" provided did not match levels available ({LOG_LEVELS}).")
            sys.exit()  
        logger = logging.getLogger(__name__)
        
        logger.level = level
        # TODO improve the distribution of this logger
        db_tools.logger = logger
        sql_fabric_copy_helper.logger = logger
        onelake_tools.logger = logger
    
    del args["log_level"]
    
    if logger: logger.debug(f"{args=}")
    else: debug(f"{args=}")
    
    if " from " in args["source"].lower() and not args["target_table"]:
        raise Exception("If source provided is a query, you MUST pass a target_table.")
    if not args["storage_account"]:
        args["storage_account"] = "onelake"
    
    upload_table_lakehouse(
        **args
    )
