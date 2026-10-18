$ErrorActionPreference = "Continue"
$failed = $false

foreach ($name in @("java", "javac", "mvn", "hadoop", "python")) {
    $tool = Get-Command $name -ErrorAction SilentlyContinue
    if ($tool) {
        Write-Output ("{0,-8} {1}" -f $name, $tool.Source)
    } else {
        Write-Output ("{0,-8} MISSING" -f $name)
        $failed = $true
    }
}

java -version
python --version
if (Get-Command hadoop -ErrorAction SilentlyContinue) { hadoop version }
if ($failed) { exit 1 }