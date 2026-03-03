#!/usr/bin/env bash
# test_install_ps1.sh - Targeted behavioral checks for install.ps1 helper logic

E2E_SUITE="install_ps1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../scripts/e2e_lib.sh
source "${SCRIPT_DIR}/../../scripts/e2e_lib.sh"

e2e_init_artifacts
e2e_banner "PowerShell Installer Helper Suite"

if ! command -v pwsh >/dev/null 2>&1; then
    e2e_skip "pwsh not found; skipping install.ps1 helper checks"
    e2e_summary
    exit 0
fi

WORK="$(e2e_mktemp "e2e_install_ps1")"
INSTALL_PS1="${SCRIPT_DIR}/../../install.ps1"

run_pwsh_case() {
    local case_id="$1"
    local case_title="$2"
    local body="$3"
    local ps_file="${WORK}/${case_id}.ps1"
    local out_file="${WORK}/${case_id}.out"
    local out rc

    cat > "${ps_file}" <<'PS_SCRIPT'
param([string]$InstallPath)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
PS_SCRIPT
    printf '%s\n' "$body" >> "${ps_file}"

    set +e
    pwsh -NoLogo -NoProfile -File "${ps_file}" "${INSTALL_PS1}" >"${out_file}" 2>&1
    rc=$?
    set -e

    out="$(cat "${out_file}" 2>/dev/null || true)"
    e2e_save_artifact "${case_id}.out.txt" "${out}"

    if [ "$rc" -eq 0 ]; then
        e2e_pass "${case_title}"
    else
        e2e_fail "${case_title}" "pwsh exit 0" "pwsh exit ${rc}"
        if [ -n "$out" ]; then
            printf '%s\n' "$out"
        fi
    fi
}

e2e_case_banner "1) install.ps1 parses cleanly"
run_pwsh_case \
    "case_01_parse" \
    "PowerShell parser has zero errors" \
'
$tokens = $null
$errors = $null
[void][System.Management.Automation.Language.Parser]::ParseFile(
    $InstallPath,
    [ref]$tokens,
    [ref]$errors
)
if ($errors.Count -gt 0) {
    $errors | ForEach-Object { Write-Error $_.Message }
    throw "parse errors detected"
}
'

e2e_case_banner "2) Normalize-Version extracts semantic versions from real output"
run_pwsh_case \
    "case_02_normalize" \
    "Normalize-Version handles prefixed and annotated version strings" \
'
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    $InstallPath,
    [ref]$tokens,
    [ref]$errors
)
if ($errors.Count -gt 0) {
    throw "failed to parse install.ps1"
}
function Load-InstallFunction([string]$Name) {
    $fnAst = $ast.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst]
    }, $true) | Where-Object { $_.Name -eq $Name } | Select-Object -First 1
    if ($null -eq $fnAst) {
        throw "function not found: $Name"
    }
    Invoke-Expression ("function global:{0} {1}" -f $Name, $fnAst.Body.Extent.Text)
}
Load-InstallFunction "Normalize-Version"

$cases = @(
    @{ Input = "v1.2.3"; Expected = "1.2.3" },
    @{ Input = "am 0.2.0"; Expected = "0.2.0" },
    @{ Input = "mcp-agent-mail 2.3.4-beta.1+abc"; Expected = "2.3.4-beta.1+abc" }
)
foreach ($case in $cases) {
    $actual = Normalize-Version -RawVersion $case.Input
    if ($actual -ne $case.Expected) {
        throw "Normalize-Version mismatch for input [$($case.Input)]: expected [$($case.Expected)] got [$actual]"
    }
}
'

e2e_case_banner "3) Checksum helpers validate good hash and reject bad hash"
run_pwsh_case \
    "case_03_checksum" \
    "Verify-ChecksumFile succeeds on matching hash and fails on mismatch" \
'
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    $InstallPath,
    [ref]$tokens,
    [ref]$errors
)
if ($errors.Count -gt 0) {
    throw "failed to parse install.ps1"
}
function Load-InstallFunction([string]$Name) {
    $fnAst = $ast.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst]
    }, $true) | Where-Object { $_.Name -eq $Name } | Select-Object -First 1
    if ($null -eq $fnAst) {
        throw "function not found: $Name"
    }
    Invoke-Expression ("function global:{0} {1}" -f $Name, $fnAst.Body.Extent.Text)
}
Load-InstallFunction "Write-Ok"
Load-InstallFunction "Get-Sha256Hex"
Load-InstallFunction "Parse-ChecksumHex"
Load-InstallFunction "Verify-ChecksumFile"

$tmp = [System.IO.Path]::GetTempFileName()
try {
    Set-Content -LiteralPath $tmp -Value "checksum-test" -NoNewline
    $good = Get-Sha256Hex -FilePath $tmp
    Verify-ChecksumFile -FilePath $tmp -ExpectedChecksum $good

    $threw = $false
    try {
        Verify-ChecksumFile -FilePath $tmp -ExpectedChecksum ("0" * 64)
    } catch {
        $threw = $true
    }
    if (-not $threw) {
        throw "expected checksum mismatch to throw"
    }
} finally {
    if (Test-Path -LiteralPath $tmp) {
        Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    }
}
'

e2e_case_banner "4) Atomic installer replacement swaps binaries and rejects missing sources"
run_pwsh_case \
    "case_04_atomic" \
    "Install-BinariesAtomically replaces targets and preserves state on missing source input" \
'
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    $InstallPath,
    [ref]$tokens,
    [ref]$errors
)
if ($errors.Count -gt 0) {
    throw "failed to parse install.ps1"
}
function Load-InstallFunction([string]$Name) {
    $fnAst = $ast.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst]
    }, $true) | Where-Object { $_.Name -eq $Name } | Select-Object -First 1
    if ($null -eq $fnAst) {
        throw "function not found: $Name"
    }
    Invoke-Expression ("function global:{0} {1}" -f $Name, $fnAst.Body.Extent.Text)
}
Load-InstallFunction "Install-BinariesAtomically"

$root = Join-Path ([System.IO.Path]::GetTempPath()) ("am-atomic-test-" + [Guid]::NewGuid().ToString("N"))
$srcDir = Join-Path $root "src"
$destDir = Join-Path $root "dest"
New-Item -ItemType Directory -Path $srcDir -Force | Out-Null
New-Item -ItemType Directory -Path $destDir -Force | Out-Null
try {
    $amSrc = Join-Path $srcDir "am.exe"
    $serverSrc = Join-Path $srcDir "mcp-agent-mail.exe"
    Set-Content -LiteralPath $amSrc -Value "new-am" -NoNewline
    Set-Content -LiteralPath $serverSrc -Value "new-server" -NoNewline
    Set-Content -LiteralPath (Join-Path $destDir "am.exe") -Value "old-am" -NoNewline
    Set-Content -LiteralPath (Join-Path $destDir "mcp-agent-mail.exe") -Value "old-server" -NoNewline

    Install-BinariesAtomically -AmSource $amSrc -ServerSource $serverSrc -InstallDir $destDir

    if ((Get-Content -LiteralPath (Join-Path $destDir "am.exe") -Raw) -ne "new-am") {
        throw "am.exe was not atomically replaced"
    }
    if ((Get-Content -LiteralPath (Join-Path $destDir "mcp-agent-mail.exe") -Raw) -ne "new-server") {
        throw "mcp-agent-mail.exe was not atomically replaced"
    }

    $before = Get-Content -LiteralPath (Join-Path $destDir "am.exe") -Raw
    $missingSrc = Join-Path $srcDir "missing-server.exe"
    $threw = $false
    try {
        Install-BinariesAtomically -AmSource $amSrc -ServerSource $missingSrc -InstallDir $destDir
    } catch {
        $threw = $true
    }
    if (-not $threw) {
        throw "expected missing source to throw"
    }
    $after = Get-Content -LiteralPath (Join-Path $destDir "am.exe") -Raw
    if ($before -ne $after) {
        throw "destination mutated on missing source failure"
    }
} finally {
    if (Test-Path -LiteralPath $root) {
        Remove-Item -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue
    }
}
'

e2e_summary
