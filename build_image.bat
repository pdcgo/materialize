@echo off
setlocal enabledelayedexpansion

:: ==== CONFIGURATION ====
set PROJECT_ID=pdcgudang
set REGION=asia-southeast2
set REPOSITORY=statrepo
set IMAGE_NAME=streamstat
@REM set TAG=latest

:: ==== LOGIN TO GCP ====
@REM echo Logging in to GCP...
@REM gcloud auth login
@REM gcloud config set project %PROJECT_ID%

:: Enable Docker authentication for Artifact Registry
@REM echo Configuring Docker authentication...
@REM gcloud auth configure-docker %REGION%-docker.pkg.dev --quiet

:: ==== PUSH TO ARTIFACT REGISTRY ====
echo Build docker image to Artifact Registry...
gcloud builds submit . --tag %REGION%-docker.pkg.dev/%PROJECT_ID%/%REPOSITORY%/%IMAGE_NAME%:latest --project %PROJECT_ID%

:: ==== DONE ====
echo.
echo =======================================
echo ✅ Image build successfully!
echo =======================================
echo %REGION%-docker.pkg.dev/%PROJECT_ID%/%REPOSITORY%/%IMAGE_NAME%:latest
echo.

pause
