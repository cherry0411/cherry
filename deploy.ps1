# Cherry deploy script for Windows
# Usage: .\deploy.ps1 [all|api|frontend|crawler]
param([string]$Service = "all")

$Server = $env:CHERRY_SERVER
if (-not $Server) {
    $Server = Read-Host "Server (user@host)"
}

function Invoke-Remote {
    param([string]$Cmd)
    ssh $Server "cd /opt/cherry && $Cmd"
}

switch ($Service) {
    "all" {
        Write-Host "Deploying all..." -ForegroundColor Cyan
        Invoke-Remote "git pull && docker compose up -d --build && docker image prune -f"
    }
    "api" {
        Write-Host "Deploying API..." -ForegroundColor Cyan
        Invoke-Remote "git pull && docker compose build api && docker compose up -d --no-deps api && docker image prune -f"
    }
    "frontend" {
        Write-Host "Deploying frontend..." -ForegroundColor Cyan
        Invoke-Remote "git pull && docker compose up -d --no-deps --force-recreate frontend"
    }
    "crawler" {
        Write-Host "Deploying crawler..." -ForegroundColor Cyan
        Invoke-Remote "git pull && docker compose build crawler && docker compose up -d --no-deps crawler && docker image prune -f"
    }
    default {
        Write-Host "Usage: .\deploy.ps1 [all|api|frontend|crawler]"
    }
}

Write-Host "Done." -ForegroundColor Green
