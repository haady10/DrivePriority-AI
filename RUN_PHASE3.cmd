@echo off
setlocal EnableExtensions
REM Phase 2 profile + Phase 3 labels/features/splits.
REM Usage:  RUN_PHASE3.cmd "D:\path\to\Integrated_Data\<snapshot>" [extra build options]
REM Example (interim run while only 2016-2021 is integrated):
REM   RUN_PHASE3.cmd "D:\...\Integrated_Data\snapshot_x" --train-end 2019-12-31 --valid-end 2020-12-31 --test-end 2021-12-31
set "PACKAGE_ROOT=%~dp0"
set "PYTHONDONTWRITEBYTECODE=1"
set "PYTHONPATH=%PACKAGE_ROOT%src"
if "%~1"=="" echo Usage: RUN_PHASE3.cmd "<snapshot folder>" [options]& exit /b 2
set "SNAPSHOT=%~1"
shift
set "EXTRA="
:collect
if "%~1"=="" goto run
set "EXTRA=%EXTRA% %1"
shift
goto collect
:run
if defined DRIVEPRIORITY_PYTHON (
  set "DP_PYTHON=%DRIVEPRIORITY_PYTHON%"
) else if exist "%PACKAGE_ROOT%.venv\Scripts\python.exe" (
  set "DP_PYTHON=%PACKAGE_ROOT%.venv\Scripts\python.exe"
) else (
  for /f "delims=" %%P in ('where python 2^>nul') do if not defined DP_PYTHON set "DP_PYTHON=%%P"
)
if not defined DP_PYTHON echo ERROR: Python not found.& exit /b 2
"%DP_PYTHON%" -c "import duckdb, pyarrow" >nul 2>&1
if errorlevel 1 echo ERROR: duckdb/pyarrow missing. Run: pip install -r requirements.txt& exit /b 2

echo === Step 1/2: Phase 2 data profile (all rows) ===
"%DP_PYTHON%" -B -m drivepriority.analysis.profile --snapshot "%SNAPSHOT%" --out "%PACKAGE_ROOT%results\phase2_profile" --temp-dir "%PACKAGE_ROOT%results\_duckdb_tmp"
if errorlevel 1 echo ERROR: profile failed.& exit /b 1

echo === Step 2/2: Phase 3 labels, features, splits ===
"%DP_PYTHON%" -B -m drivepriority.features.build_dataset --snapshot "%SNAPSHOT%" --out "%PACKAGE_ROOT%data\processed\phase3" --temp-dir "%PACKAGE_ROOT%results\_duckdb_tmp" %EXTRA%
if errorlevel 1 echo ERROR: build failed.& exit /b 1

echo.
echo DONE. Send these back to Claude:
echo   results\phase2_profile\summary.json  and the CSV files in that folder
echo   data\processed\phase3\phase3_manifest.json
exit /b 0
