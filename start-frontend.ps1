<#
.SYNOPSIS
  Start the SPA frontend using npx serve (PowerShell wrapper).

.DESCRIPTION
  Runs `npx serve -s frontend -l 3000` to serve the `frontend` folder on port 3000.

.USAGE
  > .\scripts\start-frontend.ps1
#>

# Ensure npx is available and run the static server
Write-Output "Starting frontend: npx serve -s frontend -l 3000"
npx serve -s frontend -l 3000
