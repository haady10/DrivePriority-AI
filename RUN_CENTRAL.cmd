@echo off
setlocal EnableExtensions
set "PACKAGE_ROOT=%~dp0"
set "PYTHONDONTWRITEBYTECODE=1"
set "PYTHONPATH=%PACKAGE_ROOT%src"
if defined DRIVEPRIORITY_PYTHON (
  set "DP_PYTHON=%DRIVEPRIORITY_PYTHON%"
) else if exist "%PACKAGE_ROOT%.venv\Scripts\python.exe" (
  set "DP_PYTHON=%PACKAGE_ROOT%.venv\Scripts\python.exe"
) else (
  for /f "delims=" %%P in ('where python 2^>nul') do if not defined DP_PYTHON set "DP_PYTHON=%%P"
)
if not defined DP_PYTHON echo ERROR: Python not found.& exit /b 2
"%DP_PYTHON%" -c "import duckdb, pyarrow, yaml" >nul 2>&1
if errorlevel 1 echo ERROR: Required pinned dependencies are unavailable.& exit /b 2
"%DP_PYTHON%" -B -m drivepriority.integration.cli %*
exit /b %ERRORLEVEL%
