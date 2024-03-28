$ErrorActionPreference = "Stop"
$name = "fabric_copy"
$venv_path = ".venv"

$reset_pwd = $PWD
Set-Location $PSScriptRoot

if(-Not (Test-Path $venv_path)) {
    Write-Error "No .venv folder found. Please run Setup.ps1 first."
    exit 1
}
$deploy_path = "deploy"
if(-Not (Test-Path $deploy_path)) {    
    New-Item -Path $deploy_path -ItemType Directory
}
$zip_path = "$deploy_path/${name}_deployment.zip"

if(Test-Path $zip_path) {
    Remove-Item $zip_path
}

Compress-Archive -Path $venv_path, "fabric_copy", "Run.ps1" -DestinationPath $zip_path


Set-Location $reset_pwd