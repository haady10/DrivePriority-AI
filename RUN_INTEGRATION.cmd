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
  echo ERROR: Python was not found. Set DRIVEPRIORITY_PYTHON to the approved Python 3.11 executable.
  set "DP_EXIT=2"
  goto :finish
)
"%DP_PYTHON%" -c "import duckdb, pyarrow" >nul 2>&1
if errorlevel 1 (
  echo ERROR: Required pinned dependencies are unavailable in %DP_PYTHON%.
  set "DP_EXIT=2"
  goto :finish
)
if not exist "%PACKAGE_ROOT%Input_Quarters" mkdir "%PACKAGE_ROOT%Input_Quarters"
if not exist "%PACKAGE_ROOT%Integrated_Data" mkdir "%PACKAGE_ROOT%Integrated_Data"
if not exist "%PACKAGE_ROOT%Reports" mkdir "%PACKAGE_ROOT%Reports"
echo DrivePriority AI incremental central integration
echo Input:  %PACKAGE_ROOT%Input_Quarters
echo Output: %PACKAGE_ROOT%Integrated_Data
echo Reports: %PACKAGE_ROOT%Reports
echo.
"%DP_PYTHON%" -B -m drivepriority.integration.operator --root "%PACKAGE_ROOT%."
set "DP_EXIT=%ERRORLEVEL%"
:finish
echo.
if "%DP_EXIT%"=="0" (echo Integration finished successfully.) else (echo Integration stopped or completed with blocked packages. Exit code: %DP_EXIT%)
echo Read the newest report in "%PACKAGE_ROOT%Reports".
if not defined DRIVEPRIORITY_NO_PAUSE pause
exit /b %DP_EXIT%
