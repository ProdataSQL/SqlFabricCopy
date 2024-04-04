# Sample Use
# ./SqlFabricCopy.ps1 -sql_server localhost -database_name AdventureWorksDW -source aw.DimCurrency -workspace_name "FabricDW [Dev]" -lakehouse_name FabricLH 
# ./SqlFabricCopy.ps1 -sql_server localhost -database_name AdventureWorksDW -source "aw.DimCurrency,aw.DimAccount"  -workspace_name "FabricDW [Dev]" -lakehouse_name FabricLH 
# ./SqlFabricCopy.ps1 -sql_server localhost -database_name AdventureWorksDW -source "SELECT * FROM aw.DimAccount" -target_table DimAccount -workspace_name "FabricDW [Dev]" -lakehouse_name FabricLH 
# ./SqlFabricCopy.ps1 -sql_server localhost -database_name AdventureWorksDW -source "aw.DimCurrency,aw.DimAccount"  -workspace_name "FabricDW [Dev]" -lakehouse_name FabricLH -log_level DEBUG


param (
    [Parameter(Mandatory=$true)]
    [string]$sql_server,
    [Parameter(Mandatory=$true)]
    [string]$database_name,
    [Parameter(Mandatory=$true)]
    [string]$source,
    [Parameter(Mandatory=$true)]
    [string]$workspace_name,
    [Parameter(Mandatory=$true)]
    [string]$lakehouse_name,
    [string]$target_table,
    [string]$tenant_id,
    [string]$client_id,
    [string]$client_secret,
    [string]$log_level
)

$arg_list = @()

$arg_list += "--sql_server", $sql_server
$arg_list += "--database_name", $database_name
$arg_list += "--source", $source
$arg_list += "--workspace_name", $workspace_name
$arg_list += "--lakehouse_name", $lakehouse_name



if (-not [string]::IsNullOrWhiteSpace($target_table)) {
    $arg_list += "--target_table", $target_table
}
if (-not [string]::IsNullOrWhiteSpace($log_level)) {
    $arg_list += "--log_level", $log_level
}
if (-not [string]::IsNullOrWhiteSpace($tenant_id) -or -not [string]::IsNullOrWhiteSpace($client_id) -or -not [string]::IsNullOrWhiteSpace($client_secret)) {
    if ([string]::IsNullOrWhiteSpace($tenant_id) -or [string]::IsNullOrWhiteSpace($client_id) -or [string]::IsNullOrWhiteSpace($client_secret)) {
        throw "If tenant_id, client_id or client_secret are provided, you must rovide all of the other variables required for TokenClient authentication."
    }
    $arg_list += "--tenant_id", $tenant_id
    $arg_list += "--client_id", $client_id
    $arg_list += "--client_secret", $client_secret
}
 
$ErrorActionPreference = "Stop"


$reset_pwd = $PWD
Set-Location $PSScriptRoot

$venv_path = ".venv"

if (!(Test-Path $venv_path)) {
    Write-Error "No .venv folder found. Please run Setup.ps1 first."
    exit 1
}

$OLD_PATH = $env:Path

# defaults to using the VENV python and not the system python
$env:Path = ".venv\Scripts;$env:Path"

python -m sql_fabric_copy @arg_list

Set-Location $reset_pwd
$env:Path = $OLD_PATH
