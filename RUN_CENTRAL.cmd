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
if /i "%~1"=="integrate" goto integrate
if /i "%~1"=="incremental" goto incremental
if /i "%~1"=="validate" goto validate
if /i "%~1"=="admit" goto admit
echo Usage:
echo   RUN_CENTRAL.cmd admit --output-root DIR --quarter YYYYQn [trust arguments]
echo   RUN_CENTRAL.cmd integrate --sources-json FILE --output DIR
echo   RUN_CENTRAL.cmd incremental --existing DIR --new-sources-json FILE --output DIR
echo   RUN_CENTRAL.cmd validate --root DIR --report FILE
exit /b 0
:integrate
shift
"%DP_PYTHON%" -B -m drivepriority.integration.central_writer %*
exit /b %ERRORLEVEL%
:incremental
shift
"%DP_PYTHON%" -B -m drivepriority.integration.incremental %*
exit /b %ERRORLEVEL%
:validate
shift
"%DP_PYTHON%" -B -m drivepriority.integration.central_validate %*
exit /b %ERRORLEVEL%
:admit
shift
"%DP_PYTHON%" -B -m drivepriority.integration.admission %*
exit /b %ERRORLEVEL%
