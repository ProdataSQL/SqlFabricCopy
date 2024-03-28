# Get all Python executables in the PATH
$pythons = Get-Command python* -ErrorAction SilentlyContinue | Where-Object { $_.Path -like "*\python.exe" }

if ($pythons -eq $null) {
    Write-Host "No Python executables found in the PATH."
    exit
}

Write-Host "Select the desired Python version to use:"
for ($i = 0; $i -lt $pythons.Count; $i++) {
    Write-Host "$i`: $($pythons[$i].Path)"
}

$selection = Read-Host "Enter the number corresponding to the desired Python version"
$selectedPython = $pythons[$selection]

if ($selectedPython -eq $null) {
    Write-Host "Invalid selection."
    exit
}

Write-Host "selected: $($selectedPython.Path)"



& $selectedPython.Path -m pip freeze >> backup_requirements.txt

& $selectedPython.Path -m pip uninstall -y -r backup_requirements.txt