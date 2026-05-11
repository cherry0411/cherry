# Merge cloud.dump into local cherry DB — with backup, dedup, and safety checks
# Usage: powershell -ExecutionPolicy Bypass -File scripts/merge-cloud-db.ps1
$ErrorActionPreference = "Stop"

$pgBin = "C:\Program Files\PostgreSQL\18\bin"
$pg = "$pgBin\psql"
$pgDump = "$pgBin\pg_dump"
$pgRestore = "$pgBin\pg_restore"

$dumpFile = "C:\Workspace\cloud.dump"
$backupFile = "C:\Workspace\cherry_backup_$(Get-Date -Format 'yyyyMMdd_HHmmss').dump"
$hashesFile = "$env:TEMP\cherry_hashes.txt"
$tempDump = "$env:TEMP\cherry_tmp_clean.dump"
$mainDB = "cherry"
$tmpDB = "cherry_tmp_merge"
$pgUser = "postgres"
$pgPort = 5432

# ---- preflight ----
if (-not (Test-Path $dumpFile)) { throw "cloud.dump not found: $dumpFile" }
Write-Host "=== Step 0: Backup $mainDB → $backupFile" -ForegroundColor Cyan
& $pgDump -U $pgUser -p $pgPort -d $mainDB -Fc -f $backupFile
Write-Host "  Backup: $backupFile ($((Get-Item $backupFile).Length / 1MB) MB)" -ForegroundColor Green

# ---- Step 1: Restore cloud.dump into temp DB ----
Write-Host "=== Step 1: Restore cloud.dump → $tmpDB" -ForegroundColor Cyan
& $pg -U $pgUser -p $pgPort -c "DROP DATABASE IF EXISTS $tmpDB"
& $pg -U $pgUser -p $pgPort -c "CREATE DATABASE $tmpDB"
Write-Host "  Restoring... (may take a while)"
& $pgRestore -U $pgUser -p $pgPort -d $tmpDB --clean --if-exists $dumpFile 2>&1 | Select-Object -Last 5
Write-Host "  Done" -ForegroundColor Green

# ---- Step 2: Export main DB hashes to file ----
Write-Host "=== Step 2: Export $mainDB hashes" -ForegroundColor Cyan
& $pg -U $pgUser -p $pgPort -d $mainDB -t -A -c "SELECT info_hash FROM torrents" -o $hashesFile
$hashCount = (Get-Content $hashesFile | Measure-Object -Line).Lines
Write-Host "  Exported $hashCount hashes" -ForegroundColor Green

# ---- Step 3: Load hashes into temp DB + dedup ----
Write-Host "=== Step 3: Deduplicate $tmpDB" -ForegroundColor Cyan
& $pg -U $pgUser -p $pgPort -d $tmpDB -c "
  CREATE TABLE _hashes (info_hash varchar(40));
  COPY _hashes FROM '$($hashesFile.Replace('\', '/'))';
  CREATE INDEX _idx_h ON _hashes(info_hash);
"
& $pg -U $pgUser -p $pgPort -d $tmpDB -c "
  DELETE FROM torrent_files WHERE info_hash IN (SELECT info_hash FROM _hashes);
  DELETE FROM torrents WHERE info_hash IN (SELECT info_hash FROM _hashes);
  DROP TABLE _hashes;
"
Write-Host "  Done" -ForegroundColor Green

# ---- Step 4: Dump cleaned temp DB ----
Write-Host "=== Step 4: Dump cleaned $tmpDB" -ForegroundColor Cyan
& $pgDump -U $pgUser -p $pgPort -d $tmpDB --data-only --on-conflict-do-nothing -f $tempDump
Write-Host "  Temp dump: $tempDump ($((Get-Item $tempDump).Length / 1KB) KB)" -ForegroundColor Green

# ---- Step 5: Import into main DB ----
Write-Host "=== Step 5: Import into $mainDB" -ForegroundColor Cyan
& $pg -U $pgUser -p $pgPort -d $mainDB -c "
  CREATE UNIQUE INDEX IF NOT EXISTS _tfuq ON torrent_files (info_hash, path_text)
"
Write-Host "  Importing... (may take a while)"
& $pg -U $pgUser -p $pgPort -d $mainDB -f $tempDump
& $pg -U $pgUser -p $pgPort -d $mainDB -c "DROP INDEX IF EXISTS _tfuq"
Write-Host "  Done" -ForegroundColor Green

# ---- Step 6: Cleanup ----
Write-Host "=== Step 6: Cleanup" -ForegroundColor Cyan
& $pg -U $pgUser -p $pgPort -c "DROP DATABASE IF EXISTS $tmpDB"
Remove-Item -Force $hashesFile, $tempDump -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "=== Done ===" -ForegroundColor Green
Write-Host "Backup: $backupFile"
Write-Host "New total: $((& $pg -U $pgUser -p $pgPort -d $mainDB -t -c 'SELECT count(*) FROM torrents').TrimEnd()) torrents"
