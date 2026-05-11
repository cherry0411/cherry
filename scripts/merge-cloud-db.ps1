# Merge cloud.dump into local cherry DB — with backup, dedup, and safety checks
# Usage: powershell -ExecutionPolicy Bypass -File scripts/merge-cloud-db.ps1
# Requirements: cloud.dump at C:\Workspace\cloud.dump

$ErrorActionPreference = "Stop"

$pgBin = "C:\Program Files\PostgreSQL\18\bin"
$pg    = "$pgBin\psql"
$pgDump= "$pgBin\pg_dump"
$pgRest= "$pgBin\pg_restore"

$dumpFile   = "C:\Workspace\cloud.dump"
$backupFile = "C:\Workspace\cherry_backup_$(Get-Date -Format 'yyyyMMdd_HHmmss').dump"
$hashesFile = "$env:TEMP\cherry_hashes.txt"
$tempDump   = "$env:TEMP\cherry_tmp_clean.dump"
$mainDB     = "cherry"
$tmpDB      = "cherry_tmp_merge"
$pgUser     = "postgres"
$pgPass     = $env:PGPASSWORD
$pgPort     = 5432

if (-not $pgPass) {
    $pgPass = Read-Host "Enter PostgreSQL password for $pgUser" -AsSecureString
    $pgPass = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($pgPass))
}
$env:PGPASSWORD = $pgPass

# ---- preflight ----
if (-not (Test-Path $dumpFile)) { throw "cloud.dump not found at $dumpFile" }

# ---- Step 0: Backup main DB ----
Write-Host "=== Step 0: Backup $mainDB" -ForegroundColor Cyan
$before = (& $pg -U $pgUser -p $pgPort -d $mainDB -t -c "SELECT count(*) FROM torrents").Trim()
& $pgDump -U $pgUser -p $pgPort -d $mainDB -Fc -f $backupFile
$backupSize = [math]::Round((Get-Item $backupFile).Length / 1MB, 1)
Write-Host "  Backup OK: $backupFile ($backupSize MB) — torrents before: $before" -ForegroundColor Green

# ---- Step 1: Restore cloud.dump → temp DB ----
Write-Host "=== Step 1: Restore cloud.dump → $tmpDB" -ForegroundColor Cyan
& $pg -U $pgUser -p $pgPort -c "DROP DATABASE IF EXISTS $tmpDB"
& $pg -U $pgUser -p $pgPort -c "CREATE DATABASE $tmpDB"
& $pgRest -U $pgUser -p $pgPort -d $tmpDB --clean --if-exists $dumpFile
$tmpTotal = (& $pg -U $pgUser -p $pgPort -d $tmpDB -t -c "SELECT count(*) FROM torrents").Trim()
Write-Host "  Temp DB ready: $tmpTotal torrents" -ForegroundColor Green

# ---- Step 2: Export main DB hashes ----
Write-Host "=== Step 2: Export $mainDB hashes" -ForegroundColor Cyan
& $pg -U $pgUser -p $pgPort -d $mainDB -t -A -c "SELECT info_hash FROM torrents" -o $hashesFile
$hashCount = (Get-Content $hashesFile -ErrorAction SilentlyContinue | Measure-Object -Line).Lines
Write-Host "  Exported $hashCount hashes" -ForegroundColor Green

# ---- Step 3: Dedup in temp DB ----
Write-Host "=== Step 3: Dedup $tmpDB (remove $hashCount known hashes)" -ForegroundColor Cyan
# Must run in ONE session because _h is a regular table shared across DELETE statements
& $pg -U $pgUser -p $pgPort -d $tmpDB -c "
  CREATE TABLE IF NOT EXISTS _h (info_hash varchar(40) PRIMARY KEY);
  TRUNCATE _h;
  COPY _h FROM '$($hashesFile.Replace('\', '/'))';
  ANALYZE _h;
  DELETE FROM torrent_files WHERE info_hash IN (SELECT info_hash FROM _h);
  DELETE FROM torrents WHERE info_hash IN (SELECT info_hash FROM _h);
  DROP TABLE _h;
"
$tmpRemain = (& $pg -U $pgUser -p $pgPort -d $tmpDB -t -c "SELECT count(*) FROM torrents").Trim()
Write-Host "  Temp DB after dedup: $tmpRemain torrents (will be merged)" -ForegroundColor Yellow

# ---- Step 4: Dump cleaned temp DB (custom format = COPY-based, fast) ----
Write-Host "=== Step 4: Dump cleaned $tmpDB" -ForegroundColor Cyan
& $pgDump -U $pgUser -p $pgPort -d $tmpDB --data-only -Fc -f $tempDump
$tmpSize = [math]::Round((Get-Item $tempDump).Length / 1MB, 1)
Write-Host "  Temp dump: $tempDump ($tmpSize MB)" -ForegroundColor Green

# ---- Step 5: Restore into main DB (COPY-based, fast) ----
Write-Host "=== Step 5: Import into $mainDB" -ForegroundColor Cyan
& $pgRest -U $pgUser -p $pgPort -d $mainDB --data-only --single-transaction $tempDump
$after = (& $pg -U $pgUser -p $pgPort -d $mainDB -t -c "SELECT count(*) FROM torrents").Trim()
$added = [int]$after - [int]$before
Write-Host "  Import OK: $before → $after (+$added torrents)" -ForegroundColor Green

# ---- Step 6: Verify no duplicates ----
Write-Host "=== Step 6: Verify" -ForegroundColor Cyan
$dupTorrents = (& $pg -U $pgUser -p $pgPort -d $mainDB -t -c "
  SELECT count(*) FROM torrents GROUP BY info_hash HAVING count(*) > 1
").Trim()
$dupFiles = (& $pg -U $pgUser -p $pgPort -d $mainDB -t -c "
  SELECT count(*) FROM torrent_files GROUP BY info_hash, path_text HAVING count(*) > 1
").Trim()
if ($dupTorrents -and $dupTorrents -ne "0") { Write-Host "  ⚠ torrents duplicates found" -ForegroundColor Red }
else { Write-Host "  ✓ No duplicate torrents" -ForegroundColor Green }
if ($dupFiles -and $dupFiles -ne "0") { Write-Host "  ⚠ files duplicates found" -ForegroundColor Red }
else { Write-Host "  ✓ No duplicate files" -ForegroundColor Green }

# ---- Step 7: Cleanup ----
Write-Host "=== Step 7: Cleanup" -ForegroundColor Cyan
& $pg -U $pgUser -p $pgPort -c "DROP DATABASE IF EXISTS $tmpDB"
Remove-Item -Force $hashesFile, $tempDump -ErrorAction SilentlyContinue
Write-Host "  Temp DB and files removed" -ForegroundColor Green

Write-Host ""
Write-Host "=== Done: $before → $after (+$added) ===" -ForegroundColor Green
Write-Host "Backup: $backupFile"
Write-Host "  Restore if needed: pg_restore -U postgres -d cherry --clean $backupFile"
