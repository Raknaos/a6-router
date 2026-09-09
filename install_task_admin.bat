@echo off
REM ============================================================
REM  A6Router : passer la tache planifiee en mode S4U (boot sans
REM  session ouverte, aucun mot de passe stocke) + supprimer le
REM  kill 72h + neutraliser les reglages batterie.
REM  A EXECUTER EN TANT QU'ADMINISTRATEUR (clic droit).
REM  Idempotent : re-executable sans risque.
REM ============================================================
net session >nul 2>&1
if %errorlevel% neq 0 (
  echo [ERREUR] Ce script doit etre execute EN TANT QU'ADMINISTRATEUR.
  echo          Clic droit sur le fichier ^> "Executer en tant qu'administrateur"
  pause
  exit /b 1
)

set TASKDIR=%USERPROFILE%\Documents\Projets_Hermes\a6-router
set XML=%TASKDIR%\A6Router-task.xml

if not exist "%XML%" (
  echo [ERREUR] XML introuvable : %XML%
  pause
  exit /b 1
)

echo [1/3] Suppression de l'ancienne tache A6Router...
schtasks /delete /tn A6Router /f >nul 2>&1

echo [2/3] Creation de la tache v2 (S4U = demarre au boot sans session)...
schtasks /create /tn A6Router /xml "%XML%"
if %errorlevel% neq 0 (
  echo [ERREUR] Creation impossible.
  pause
  exit /b 1
)

echo [3/3] Demarrage immediat...
schtasks /run /tn A6Router >nul 2>&1
timeout /t 6 /nobreak >nul
schtasks /query /tn A6Router /v /fo list | findstr /i "Status Result Logon"
echo.
echo === VERIFICATION : le routeur doit repondre ===
curl -s -m 5 http://127.0.0.1:8791/health
echo.
if %errorlevel% equ 0 (
  echo [OK] Tache A6Router v2 installee et routeur actif.
) else (
  echo [ATTENTION] Routeur non repondu - verifier router.log
)
pause
