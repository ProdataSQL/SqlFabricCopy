# Sample Use
# ./Run.ps1 -sql_server localhost -database_name AdventureWorksDW -schema_name aw -table_name "DimCurrency,DimAccount" -workspace_name "FabricDW [Dev]" -lakehouse_name FabricLH 
param (
    [Parameter(Mandatory=$true)]
    [string]$sql_server,
    [Parameter(Mandatory=$true)]
    [string]$database_name,
    [Parameter(Mandatory=$true)]
    [string]$schema_name,
    [Parameter(Mandatory=$true)]
    [string]$table_name,
    [Parameter(Mandatory=$true)]
    [string]$workspace_name,
    [Parameter(Mandatory=$true)]
    [string]$lakehouse_name,
    
    [string]$tenant_id,
    [string]$client_id,
    [string]$client_secret
)

$ErrorActionPreference = "Stop"


$reset_pwd = $PWD
# used to execute on target, or to test
# move to relative directory
Set-Location $PSScriptRoot

$venv_path = ".venv"

if (!(Test-Path $venv_path)) {
    Write-Error "No .venv folder found. Please run Setup.ps1 first."
    exit 1
}

$OLD_PATH = $env:Path
# defaults to using the VENV python and not the system python
$env:Path = ".venv\Scripts;$env:Path"
if ($tenant_id){
    python -m fabric_copy --sql_server $sql_server --database_name $database_name --schema_name $schema_name --table_name $table_name --workspace_name "$workspace_name" --lakehouse_name $lakehouse_name --tenant_id $tenant_id --client_id $client_id --client_secret $client_secret
} else {
    python -m fabric_copy --sql_server $sql_server --database_name $database_name --schema_name $schema_name --table_name $table_name --workspace_name "$workspace_name" --lakehouse_name $lakehouse_name
}

Set-Location $reset_pwd
$env:Path = $OLD_PATH

Write-Host "Press any key to continue..."