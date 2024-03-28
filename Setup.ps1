# This must be executed first on a machine that has python installed -> it will install python into the .venv folder
# which can then be zipped and deployed to the target machine with the ZipDeploy.ps1 script

# it is HIGHLY recommended you use a CLEAN python install
# A two liner to first clean python install:
# pip freeze > requirements.txt
# pip uninstall -y -r requirements.txt

# This script will create a virtual environment and install the requirements from the requirements.txt file
$ErrorActionPreference = "Stop"
$venv_path = ".venv"
$reset_pwd = $PWD
Set-Location $PSScriptRoot
$process = Get-Process "Code" -ErrorAction SilentlyContinue
if ($process -ne $null) {
    
    $response = Read-Host "This install script will cause issues because Visual Studio Code is running. Do you want to continue? (y/[n])"
    $continue_prompt = $true
    while ($continue_prompt){
        if ($response -eq "y" -or $response -eq "Y" ) {
            $continue_prompt = $false
        } elseif ($response -eq "n" -or $response -eq "N" -or [string]::IsNullOrEmpty($response)) {
            exit
        } else {
            Write-Host "Invalid input. Please enter 'y' for yes or 'n' for no."
        }
    }
}
if(Test-Path $venv_path) {
    Remove-Item $venv_path -Recurse
}

python -m venv .venv --copies --clear

./.venv/Scripts/Activate.ps1

pip install -r requirements.txt



Set-Location $reset_pwd

pause