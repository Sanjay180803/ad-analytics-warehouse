<#
  Windows PowerShell version of the Makefile.

  Usage (from the project folder):
    .\run.ps1 setup          one time: venv, Python packages, Hadoop winutils for Spark
    .\run.ps1 sample         make a 500K-row synthetic Criteo file
    .\run.ps1 land-local     land it into data\landing (no AWS needed)
    .\run.ps1 spark-local    clean one day locally
    .\run.ps1 test           run the Spark tests
    .\run.ps1 land-s3        land the REAL Criteo file into S3
    .\run.ps1 spark-day      clean one day S3 -> S3          (-Day 2025-01-05)
    .\run.ps1 benchmark      before/after Spark timings
    .\run.ps1 keys           generate the Snowflake key pair
    .\run.ps1 snowflake-setup  run the 3 Snowflake setup scripts (needs -AwsRoleArn)
    .\run.ps1 astro-start    start local Airflow
    .\run.ps1 backfill       load all 31 days through Airflow
    .\run.ps1 backfill-day   rerun one day                   (-Day 2025-01-07)

  If PowerShell refuses to run scripts, run this once first:
    Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
#>
param(
    [Parameter(Position = 0)][string]$Task = "help",
    [string]$Day = "2025-01-05",
    [string]$AwsRoleArn = ""
)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# ---- load .env into this session (KEY=VALUE lines; quotes stripped) ----------
if (Test-Path .env) {
    Get-Content .env | ForEach-Object {
        if ($_ -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$') {
            $v = $Matches[2].Trim()
            if ($v.Length -ge 2 -and (($v[0] -eq "'" -and $v[-1] -eq "'") -or ($v[0] -eq '"' -and $v[-1] -eq '"'))) {
                $v = $v.Substring(1, $v.Length - 2)
            }
            Set-Item -Path "Env:$($Matches[1])" -Value $v
        }
    }
}

# ---- Python from the project venv, Hadoop helper for Spark on Windows ---------
$Py = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
$HadoopHome = Join-Path $HOME "hadoop"
if (Test-Path "$HadoopHome\bin\winutils.exe") {
    $env:HADOOP_HOME = $HadoopHome
    $env:PATH = "$HadoopHome\bin;$env:PATH"
}
$env:PYSPARK_PYTHON = $Py
$env:PYSPARK_DRIVER_PYTHON = $Py

$Raw   = "s3://$($env:S3_BUCKET)/raw/criteo"
$Clean = "s3://$($env:S3_BUCKET)/clean/criteo"

function Need-Bucket { if (-not $env:S3_BUCKET) { throw "S3_BUCKET is not set. Fill in .env first." } }
function Run($exe, [string[]]$argv) {
    & $exe @argv
    if ($LASTEXITCODE -ne 0) { throw "command failed ($LASTEXITCODE): $exe $($argv -join ' ')" }
}

switch ($Task) {
    "setup" {
        Run "python" @("-m", "venv", ".venv")
        Run $Py @("-m", "pip", "install", "--upgrade", "pip")
        Run $Py @("-m", "pip", "install", "pyspark==4.2.0", "pandas<3", "numpy", "pyarrow", "boto3", "pytest")
        # Spark on Windows needs winutils.exe + hadoop.dll to write local files
        New-Item -ItemType Directory -Force "$HadoopHome\bin" | Out-Null
        $base = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin"
        foreach ($f in "winutils.exe", "hadoop.dll") {
            Invoke-WebRequest "$base/$f" -OutFile "$HadoopHome\bin\$f"
        }
        [Environment]::SetEnvironmentVariable("HADOOP_HOME", $HadoopHome, "User")
        Write-Host "Setup done. Check Java with: java -version  (needs 17 or newer)"
    }
    "sample"      { Run $Py @("include/scripts/generate_sample_data.py", "--rows", "500000", "--out", "data/sample.tsv.gz") }
    "land-local"  { Run $Py @("include/scripts/land_raw_to_s3.py", "--src", "data/sample.tsv.gz", "--dest", "data/landing") }
    "spark-local" { Run $Py @("include/spark/clean_impressions.py", "--raw-root", "data/landing", "--clean-root", "data/clean", "--dt", $Day) }
    "test"        { Run $Py @("-m", "pytest", "tests/test_spark_clean.py", "-q") }
    "land-s3" {
        Need-Bucket
        Run $Py @("include/scripts/land_raw_to_s3.py", "--src", "data/criteo_attribution_dataset.tsv.gz", "--dest", $Raw)
    }
    "spark-day" {
        Need-Bucket
        Run $Py @("include/spark/clean_impressions.py", "--raw-root", $Raw, "--clean-root", $Clean, "--dt", $Day)
    }
    "benchmark" {
        Need-Bucket
        Run $Py @("include/spark/benchmark.py", "--raw-root", $Raw, "--clean-root", "s3://$($env:S3_BUCKET)/bench/criteo",
                  "--repeats", "3", "--out", "docs/benchmark_results.md")
    }
    "keys" {
        # openssl ships with Git for Windows
        $openssl = "C:\Program Files\Git\usr\bin\openssl.exe"
        if (-not (Test-Path $openssl)) { $openssl = "openssl" }
        New-Item -ItemType Directory -Force include\secrets | Out-Null
        Run $openssl @("genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048", "-out", "include/secrets/rsa_key.p8")
        Run $openssl @("pkey", "-in", "include/secrets/rsa_key.p8", "-pubout", "-out", "include/secrets/rsa_key.pub")
        Write-Host "Keys written to include\secrets\"
    }
    "snowflake-setup" {
        Need-Bucket
        if (-not $AwsRoleArn) { throw "pass -AwsRoleArn arn:aws:iam::<account>:role/snowflake-criteo-reader" }
        $pub  = (Get-Content include\secrets\rsa_key.pub | Where-Object { $_ -notmatch '-----' }) -join ''
        $salt = -join ((1..48) | ForEach-Object { '{0:x}' -f (Get-Random -Maximum 16) })
        Run "snow" @("sql", "-f", "include/snowflake/00_account_setup.sql", "-D", "svc_public_key=$pub")
        Run "snow" @("sql", "-f", "include/snowflake/01_s3_integration_and_raw_tables.sql",
                     "-D", "s3_bucket=$($env:S3_BUCKET)", "-D", "aws_role_arn=$AwsRoleArn")
        Run "snow" @("sql", "-f", "include/snowflake/02_governance.sql", "-D", "hash_salt=$salt")
    }
    "astro-start"  { Run "astro" @("dev", "start") }
    "backfill" {
        Run "astro" @("dev", "run", "backfill", "create", "--dag-id", "ad_analytics_daily",
                      "--from-date", "2025-01-01", "--to-date", "2025-01-31", "--max-active-runs", "4")
    }
    "backfill-day" {
        Run "astro" @("dev", "run", "backfill", "create", "--dag-id", "ad_analytics_daily",
                      "--from-date", $Day, "--to-date", $Day, "--reprocess-behavior", "completed")
    }
    default { Get-Content $PSCommandPath -TotalCount 22 | Select-Object -Skip 1 }
}
