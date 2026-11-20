@echo off
setlocal EnableExtensions
REM Amendment A1 (post hoc, 20 Nov 2026): fair SMART baselines and the unplaced failures.
REM Usage:  RUN_POST_HOC.cmd <step> [options]
REM   steps: preflight | build | replay | summarise   (drivepriority.posthoc.fair_baselines)
REM          unplaced                                 (drivepriority.posthoc.unplaced_failures)
REM See docs/posthoc_A1_lab_pc_guide_2026-11-20.md for the exact commands.
set "PACKAGE_ROOT=%~dp0"
set "PYTHONDONTWRITEBYTECODE=1"
set "PYTHONPATH=%PACKAGE_ROOT%src"
if "%~1"=="" echo Usage: RUN_POST_HOC.cmd ^<preflight^|build^|replay^|summarise^|unplaced^> [options]& exit /b 2
set "STEP=%~1"
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
) else if exist "C:\DP-venv\Scripts\python.exe" (
  set "DP_PYTHON=C:\DP-venv\Scripts\python.exe"
) else if exist "%PACKAGE_ROOT%.venv\Scripts\python.exe" (
  set "DP_PYTHON=%PACKAGE_ROOT%.venv\Scripts\python.exe"
) else (
  for /f "delims=" %%P in ('where python 2^>nul') do if not defined DP_PYTHON set "DP_PYTHON=%%P"
)
if not defined DP_PYTHON echo ERROR: Python not found.& exit /b 2
"%DP_PYTHON%" -c "import duckdb, pyarrow, pandas, numpy" >nul 2>&1
if errorlevel 1 echo ERROR: duckdb/pyarrow/pandas/numpy missing. Run: pip install -r requirements.txt& exit /b 2

if /I "%STEP%"=="unplaced" (
  "%DP_PYTHON%" -B -m drivepriority.posthoc.unplaced_failures %EXTRA%
) else (
  "%DP_PYTHON%" -B -m drivepriority.posthoc.fair_baselines %STEP% %EXTRA%
)
if errorlevel 1 echo ERROR: step %STEP% stopped. Send the last lines above to Claude.& exit /b 1
exit /b 0
