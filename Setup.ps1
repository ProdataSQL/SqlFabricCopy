# This must be executed first on a machine that has python installed -> it will install python into the .venv folder
# which can then be zipped and deployed to the target machine with the ZipDeploy.ps1 script

# it is HIGHLY recommended you use a CLEAN python install
# A two liner to first clean python install:
# pip freeze > requirements.txt
# pip uninstall -y -r requirements.txt

# This script will create a virtual environment and install the requirements from the requirements.txt file
$ErrorActionPreference = "Stop"
# Get all Python executables in the PATH
$pythons = Get-Command python* -ErrorAction SilentlyContinue | Where-Object { $_.Path -like "*\python.exe" }

if ($null -eq $pythons) {
    Write-Host "No Python executables found in the PATH."
    exit
}

Write-Host "Select the desired Python version to use:"
for ($i = 0; $i -lt $pythons.Count; $i++) {
    Write-Host "$i`: $($pythons[$i].Path)"
}

$selection = Read-Host "Enter the number corresponding to the desired Python version"
$selectedPython = $pythons[$selection]

if ($null -eq $selectedPython) {
    Write-Host "Invalid selection."
    exit
}

Write-Host "selected: $($selectedPython.Path)"



$venv_path = ".venv"
$reset_pwd = $PWD
Set-Location $PSScriptRoot
$process = Get-Process "Code" -ErrorAction SilentlyContinue
if ($null -ne $process) {
    
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
& $selectedPython.Path -m pip install virtualenv --no-warn-script-location --user

& $selectedPython.Path -m venv .venv --copies --clear

./.venv/Scripts/Activate.ps1

python -m pip install -r requirements.txt



Set-Location $reset_pwd

pause