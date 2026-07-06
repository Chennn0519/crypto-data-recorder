# Daily backup: pull finalized (gzipped) recorder files from EC2 to the local
# E: data drive, then prune EC2 copies older than $keepDays -- but only files
# whose local MD5 matches the remote one. Live .jsonl files are never touched.
#
# Scheduled task "CryptoDataBackup" runs the live copy of this script at
# C:\Users\jonny\OneDrive\crypto-data-backup\backup_cdr.ps1 daily at 12:00;
# this repo copy is the source of truth. After editing, copy it there.

$ErrorActionPreference = "Stop"
$pem        = "C:\Users\jonny\OneDrive\桌面\啟動軟體\雲電腦\jonnychen0519.pem"
$remote     = "ubuntu@43.212.191.254"
$remoteData = "/opt/crypto-data-recorder/data"
$dst        = "E:\_data\recorder"
$keepDays   = 7
$logFile    = Join-Path $dst "backup.log"

function Write-Log($msg) {
    "$(Get-Date -Format s) $msg" | Out-File -Append -Encoding utf8 $logFile
}

New-Item -ItemType Directory -Force $dst | Out-Null

try {
    # 1. list finalized files on EC2 (previous days are already gzipped)
    $listing = & ssh -i $pem -o StrictHostKeyChecking=accept-new $remote `
        "cd $remoteData && find . -name '*.jsonl.gz' -type f -printf '%P\t%s\t%T@\n'"
    if (-not $listing) { throw "empty remote listing" }

    # 2. fetch anything missing locally (or with a size mismatch)
    $files = @()
    $fetched = 0
    foreach ($line in @($listing)) {
        $parts = $line.Trim() -split "`t"
        if ($parts.Count -lt 3) { continue }
        $rel = $parts[0]
        $size = [long]$parts[1]
        $mtime = [double]$parts[2]
        $local = Join-Path $dst ($rel -replace '/', '\')
        $files += New-Object psobject -Property @{
            Rel = $rel; Size = $size; Mtime = $mtime; Local = $local
        }
        if ((Test-Path $local) -and ((Get-Item $local).Length -eq $size)) { continue }
        New-Item -ItemType Directory -Force (Split-Path $local) | Out-Null
        & scp -i $pem -q "${remote}:$remoteData/$rel" $local
        if (-not ((Test-Path $local) -and ((Get-Item $local).Length -eq $size))) {
            throw "fetch failed: $rel"
        }
        $fetched++
    }

    # 3. prune EC2 copies older than $keepDays, only after an MD5 match
    $epoch = [datetime]::new(1970, 1, 1, 0, 0, 0, [DateTimeKind]::Utc)
    $cutoff = (Get-Date).ToUniversalTime().AddDays(-$keepDays)
    $old = @($files | Where-Object { $epoch.AddSeconds($_.Mtime) -lt $cutoff })
    $deleted = 0
    if ($old.Count -gt 0) {
        $names = ($old | ForEach-Object { $_.Rel }) -join ' '
        $hashLines = & ssh -i $pem $remote "cd $remoteData && md5sum $names"
        $remoteHash = @{}
        foreach ($h in @($hashLines)) {
            if ($h -match '^([0-9a-fA-F]{32})\s+(.+)$') {
                $remoteHash[$Matches[2].Trim()] = $Matches[1].ToLower()
            }
        }
        $toDelete = @()
        foreach ($f in $old) {
            $localHash = (Get-FileHash -Algorithm MD5 $f.Local).Hash.ToLower()
            if ($remoteHash[$f.Rel] -eq $localHash) { $toDelete += $f.Rel }
            else { Write-Log "WARN keeping $($f.Rel): remote/local hash mismatch" }
        }
        if ($toDelete.Count -gt 0) {
            $delNames = $toDelete -join ' '
            & ssh -i $pem $remote "cd $remoteData && rm -f $delNames" | Out-Null
            $deleted = $toDelete.Count
        }
    }
    Write-Log "ok: $($files.Count) remote files, fetched $fetched, pruned $deleted from EC2 (keep $keepDays days)"
} catch {
    Write-Log "ERROR: $_"
    exit 1
}
