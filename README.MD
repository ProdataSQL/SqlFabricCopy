## SQL to Fabric Copy Tool
Tool which copies a SQL Server table directly to a Fabric Lakehouse Delta Lake, using Python libraries [`deltalake`](https://delta-io.github.io/delta-rs/),`azure-storage-file-datalake`, `azure-identity`, `deltalake`, `pandas`, `pyodbc` and `pyarrow`.

Can deploy to a ZIP which will contain Python packaged within. Useful in situations where new software or Python cannot be installed machine wide.

# Sample usage of SqlFabricCopy
> **NOTE**
> Ensure permissions on your workspace are set as per [this tutorial](PREPARE.MD)

> **NOTE**
> If in deployment mode (from ZIP / Release), execute ./venv/Scripts/Activate.ps1 or ./venv/Scripts/activate.bat before continuing

```powershell
# Copy single table
python -m sql_fabric_copy --sql_server localhost --database_name AdventureWorksDW --source aw.DimCurrency --workspace_name "FabricDW [Dev]" --lakehouse_name FabricLH 

# Copy multiple comma seperated tables
python -m sql_fabric_copy --sql_server localhost --database_name AdventureWorksDW --source "aw.DimCurrency,aw.DimAccount"  --workspace_name "FabricDW [Dev]" --lakehouse_name FabricLH 

# Copy from query
python -m sql_fabric_copy --sql_server localhost --database_name AdventureWorksDW --source "SELECT * FROM aw.DimAccount" --target_table DimAccount --workspace_name "FabricDW [Dev]" --lakehouse_name FabricLH 

# Copy and enable logging
python -m sql_fabric_copy --sql_server localhost --database_name AdventureWorksDW --source "aw.DimCurrency,aw.DimAccount"  --workspace_name "FabricDW [Dev]" --lakehouse_name FabricLH --log_level DEBUG

# Copy from query with client ID and secret
python -m sql_fabric_copy --sql_server localhost --database_name AdventureWorksDW --source "SELECT * FROM aw.DimAccount" --target_table DimAccount --workspace_name "FabricDW [Dev]" --lakehouse_name FabricLH --tenant_id "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxx" --client_id "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxx" --client_secret "XXXXXXXXXXXXXXXXXXXXXXX"

```

# SqlFabricCopy Parameters:
- `sql_server`: Specifies the SQL Server instance (Mandatory).
- `database_name`: Specifies the name of the database from which to copy data (Mandatory).
- `source`: Defines the source table(s) or query to copy from. This can be a single table, a list of tables separated by commas, or a SQL query (Mandatory).
- `workspace_name`: The name of the workspace in the data lakehouse (Mandatory).
- `lakehouse_name`: The name of the lakehouse to copy data to (Mandatory).
- `target_table`: The target table in the data lakehouse. This is optional and only needed when a SQL query is specified in the source.
- `tenant_id`: Tenant ID for authentication, optional. 
- `client_id`: Client ID for authentication, optional. Required if tenant_id.
- `client_secret`: Client secret for authentication, optional. Required if tenant_id
- `log_level`: Specifies the logging level, optional.

# Development Requirements
1. Python 3.10> (only version tested)
2. pip packages in [requirements.txt](requirements.txt)

# Preparation
1. Install [Python](https://www.python.org/downloads/)
2. `pip install -r requirements`

# Preparation (to deploy to zip)
> **IMPORTANT**
> __**IT IS BOTH UNTESTED AND NOT RECOMMENDED TO USE MINICONDA / ANACONDA - YOU HAVE BEEN WARNED**__  
> **Your interpreter can be chosen at steps where this will matter**  
1.	Install [Python](https://www.python.org/downloads/)  
2.	If Python is already installed, I recommend clearing locally installed packages to reduce the size of the packages transferred into the new venv (`CleanLocalPip.ps1` will do this AND make a back up of currently installed packages into `backup_requirements.txt`)
3.	Execute .\Setup.ps1 (make sure VS code is CLOSED for this step)
4.  VS Code should now default to this new venv interpreter with required packages installed
5.  Execute ./ZipDeploy.ps1 to create a zip deployment which includes the portable virtualenvironment

# Editing Code using VS Code
By default, this library will use the virtual env created in the "Creating a Deployment ZIP File" step.
You can change this by selecting a different interpreter in the command pallette (ctrl + shift + P) and running `Python: Select Interpreter`.


# Test
Testing requires Windows Auth access to SQL server AND permissions for Managed Identity / VS Code identity / Service Principle to be configured within Fabric Workspace

`python -m unittest discover -s . -p *_tests.py` (Or through "Testing" tab on VS Code)

To configure testing variables edit the "test_config.ini" file located in `./Python`


