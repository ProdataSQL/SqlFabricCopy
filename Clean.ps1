$reset_pwd = $PWD
$venv_path = ".venv"
$deploy_path = "deploy"
$table_output = "output"
Set-Location $PSScriptRoot

if(Test-Path $venv_path) {
    Remove-Item $venv_path -Recurse
}
if(Test-Path $deploy_path) {
    Remove-Item $deploy_path -Recurse
}
if(Test-Path $table_output) {
    Remove-Item $table_output -Recurse
}


Set-Location $reset_pwd