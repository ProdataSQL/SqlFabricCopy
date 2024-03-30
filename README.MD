# SQL to Fabric Copy Tool
Tool which copies a SQL Server table directly to a Fabric Lakehouse Delta Lake, using Python libraries [`deltalake`](https://delta-io.github.io/delta-rs/),`azure-storage-file-datalake`, `azure-identity`, `deltalake`, `pandas`, `pyodbc` and `pyarrow`.

Can deploy to a ZIP which will contain Python packaged within. Useful in situations where new software  or Pythoin cannot be installed on machine wide.

## SQL to Fabric Copy Tool preperation
> **IMPORTANT**
> __**IT IS BOTH UNTESTED AND NOT RECOMMENDED TO USE MINICONDA / ANACONDA - YOU HAVE BEEN WARNED**__  

> **NOTE**
> **Your interpreter can be chosen at steps where this will matter**  
1.	Install [Python](https://www.python.org/downloads/)  
2.	If Python is already installed, I recommend clearing locally installed packages to reduce the size of the packages transferred into the new venv (`CleanLocalPip.ps1` will do this AND make a back up of currently installed packages into `backup_requirements.txt`)
3.	Execute .\Python\Setup.ps1 (make sure VS code is CLOSED for this step)
4.  VS Code should now default to this new venv interpreter with required packages installed
5.  Ensure permissions on your workspace are set as per [this tutorial](PREPARE.MD)
6.  Execute ./SqlFabricCopy.ps1 variation from samples below:
7.  Execute ./ZipDeploy.ps1 to create a zip deployment which includes the portable virtualenvironment

## Sample usage of SqlFabricCopy
```powershell
# Copy single table
./SqlFabricCopy.ps1 -sql_server localhost -database_name AdventureWorksDW -source aw.DimCurrency -workspace_name "FabricDW [Dev]" -lakehouse_name FabricLH 

# Copy multiple comma seperated tables
./SqlFabricCopy.ps1 -sql_server localhost -database_name AdventureWorksDW -source "aw.DimCurrency,aw.DimAccount"  -workspace_name "FabricDW [Dev]" -lakehouse_name FabricLH 

# Copy from query
./SqlFabricCopy.ps1 -sql_server localhost -database_name AdventureWorksDW -source "SELECT * FROM aw.DimAccount" -target_table DimAccount -workspace_name "FabricDW [Dev]" -lakehouse_name FabricLH 

# Copy and enable logging
./SqlFabricCopy.ps1 -sql_server localhost -database_name AdventureWorksDW -source "aw.DimCurrency,aw.DimAccount"  -workspace_name "FabricDW [Dev]" -lakehouse_name FabricLH -log_level DEBUG
```

## SqlFabricCopy Parameters:
- `-sql_server`: Specifies the SQL Server instance (Mandatory).
- `-database_name`: Specifies the name of the database from which to copy data (Mandatory).
- `-source`: Defines the source table(s) or query to copy from. This can be a single table, a list of tables separated by commas, or a SQL query (Mandatory).
- `-workspace_name`: The name of the workspace in the data lakehouse (Mandatory).
- `-lakehouse_name`: The name of the lakehouse to copy data to (Mandatory).
- `-target_table`: The target table in the data lakehouse. This is optional and only needed when a SQL query is specified in the source.
- `-tenant_id`: Tenant ID for authentication, optional. 
- `-client_id`: Client ID for authentication, optional. Required if tenant_id.
- `-client_secret`: Client secret for authentication, optional. Required if tenant_id
- `-log_level`: Specifies the logging level, optional.

# Build Deployment
`./Python/Setup.ps1` 

`./Python/DeployZip.ps1` (requires setup to be completed)

# Test
Testing requires Windows Auth access to SQL server AND permissions for Managed Identity / VS Code identity / Service Principle to be configured within Fabric Workspace

`python -m unittest discover -s . -p *_tests.py` (Or through "Testing" tab on VS Code)

To configure testing variables edit the "test_config.ini" file located in `./Python`


