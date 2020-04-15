REM Set development environment variables on windows
@echo off
set DOCKER_USER=mofsesam
set DOCKER_TAG=develop
set USER=mof
set DEV_URL=https://datahub-8a651472.sesam.cloud/api/
set HUBNR=8a651472
for /f "delims=" %%A in ('keyring get morten-dev %USER%') do set "JWT=%%A"