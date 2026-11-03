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

if not defined DP_PYTHON (
  echo ERROR: Python was not found. Set DRIVEPRIORITY_PYTHON to the approved python.exe.
  exit /b 2
)

"%DP_PYTHON%" -c "import duckdb, pyarrow, yaml" >nul 2>&1
if errorlevel 1 (
  echo ERROR: The selected Python does not provide the pinned DuckDB, PyArrow, and PyYAML dependencies.
  echo Selected Python: %DP_PYTHON%
  exit /b 2
)

if "%~1"=="" goto :help
if /i "%~1"=="help" goto :help
if /i "%~1"=="--help" goto :help
"%DP_PYTHON%" -B -m drivepriority.data.workflow %*
exit /b %ERRORLEVEL%

:help
echo DrivePriority AI master workflow
echo.
echo   RUN_ME.cmd inspect    --operator NAME --quarter YYYYQn --archive ZIP --source-url URL
echo   RUN_ME.cmd authorize  --quarter YYYYQn --archive ZIP --approvals FILE --approved-by "Haady Abdul Jabar" ... --confirm
echo   RUN_ME.cmd clean      --operator NAME --quarter YYYYQn --archive ZIP --output DIR --evidence DIR --approvals FILE
echo   RUN_ME.cmd analyze    --quarter-output DIR --analysis-output DIR
echo   RUN_ME.cmd revalidate --quarter-output DIR --analysis-output DIR --revalidation-output DIR
echo   RUN_ME.cmd status     --root DIR
echo   RUN_ME.cmd submissions --root DIR --quarter YYYYQn
echo   RUN_ME.cmd resource-check --quarter YYYYQn --archive ZIP --report FILE
echo.
echo Paths containing spaces are supported when enclosed in double quotes.
exit /b 0
