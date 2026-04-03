[CmdletBinding()]
param(
    [ValidateSet("Debug", "Release")]
    [string]$Configuration = "Release",
    [switch]$Rebuild,
    [switch]$SmokeTest
)

$ErrorActionPreference = "Stop"

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$harnessRoot = (Resolve-Path (Join-Path $scriptRoot "..")).Path
$repoRoot = (Resolve-Path (Join-Path $harnessRoot "..\..")).Path
$packageScript = Join-Path $scriptRoot "package-operator-gui.ps1"
$distExe = Join-Path $repoRoot "artifacts\ultimentality-pilot\operator\dist\AGRO Harness Operator.exe"

if ($Rebuild -or -not (Test-Path $distExe)) {
    & $packageScript -Configuration $Configuration -SkipSmokeTest
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

Push-Location $repoRoot
try {
    if ($SmokeTest) {
        & $distExe --smoke-test
        exit $LASTEXITCODE
    }

    Start-Process -FilePath $distExe -WorkingDirectory $repoRoot
}
finally {
    Pop-Location
}
