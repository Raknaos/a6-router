# Enregistre le pull du marche 30 j (Quota.Hub -> router) toutes les 2 min,
# et redemarre le routeur pour qu'il prenne le nouveau code.
$ErrorActionPreference = 'Stop'
$dir = 'C:\Users\bapti\Documents\Projets_Hermes\a6-router'
$py  = 'C:\Users\bapti\AppData\Local\hermes\hermes-agent\venv\Scripts\pythonw.exe'
$nom = 'A6API-Market30Pull'

Write-Host "=== 1. tache de pull ==="
$action = New-ScheduledTaskAction -Execute $py -Argument ('"' + $dir + '\_pull_market30.py"') -WorkingDirectory $dir
$trig   = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 2)
$prin   = New-ScheduledTaskPrincipal -UserId ($env:USERDOMAIN + '\' + $env:USERNAME) -LogonType S4U -RunLevel Limited
$set    = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 5)
Register-ScheduledTask -TaskName $nom -Action $action -Trigger $trig -Principal $prin -Settings $set -Force | Out-Null
Write-Host ("  enregistree : " + (Get-ScheduledTask -TaskName $nom).TaskName)

Write-Host "=== 2. arret du routeur (port 8791) ==="
$conn = Get-NetTCPConnection -LocalPort 8791 -State Listen -ErrorAction SilentlyContinue
if ($conn) {
  $pid8791 = ($conn | Select-Object -First 1).OwningProcess
  $p = Get-Process -Id $pid8791 -ErrorAction SilentlyContinue
  Write-Host ("  PID " + $pid8791 + " (" + $p.ProcessName + ") -> arret")
  Stop-Process -Id $pid8791 -Force
  Start-Sleep -Seconds 3
} else { Write-Host "  rien en ecoute sur 8791" }

Write-Host "=== 3. redemarrage par la tache A6Router ==="
Start-ScheduledTask -TaskName 'A6Router'
Start-Sleep -Seconds 12
$conn2 = Get-NetTCPConnection -LocalPort 8791 -State Listen -ErrorAction SilentlyContinue
if ($conn2) {
  $np = ($conn2 | Select-Object -First 1).OwningProcess
  Write-Host ("  routeur UP, PID " + $np)
} else { Write-Host "  /!\ 8791 toujours ferme" }

Write-Host "=== 4. premier pull immediat ==="
Start-ScheduledTask -TaskName $nom
Start-Sleep -Seconds 15
Get-ScheduledTask -TaskName $nom | Get-ScheduledTaskInfo | ForEach-Object {
  Write-Host ("  dernier resultat : " + $_.LastTaskResult + " | derniere execution : " + $_.LastRunTime)
}
Get-Content (Join-Path $dir 'market30_pull.log') -Tail 3
