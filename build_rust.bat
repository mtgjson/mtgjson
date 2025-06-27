@echo off
REM Windows batch script for building the mtgjson-rust Python extension
REM This script handles common Windows-specific issues and path problems

setlocal EnableDelayedExpansion

echo MTGJSON Rust Module Builder (Windows)
echo ====================================

REM Get the directory where this batch file is located
set "SCRIPT_DIR=%~dp0"
set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

echo Script directory: %SCRIPT_DIR%
echo Current directory: %CD%

REM Change to the script directory to ensure paths work correctly
pushd "%SCRIPT_DIR%"

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found in PATH
    echo Please install Python and make sure it's in your PATH
    goto :error
)

REM Check if the Python script exists
if not exist "build_rust.py" (
    echo ERROR: build_rust.py not found in %SCRIPT_DIR%
    echo Please make sure you're running this from the correct directory
    goto :error
)

REM Check if mtgjson-rust directory exists
if not exist "mtgjson-rust" (
    echo ERROR: mtgjson-rust directory not found
    echo Directory contents:
    dir /B
    echo.
    echo Please make sure the mtgjson-rust directory exists in the same location as this script
    goto :error
)

echo Found mtgjson-rust directory: %SCRIPT_DIR%\mtgjson-rust

REM Parse command line arguments
set "PYTHON_ARGS="
:parse_args
if "%~1"=="" goto :run_python
if /i "%~1"=="--help" (
    set "PYTHON_ARGS=%PYTHON_ARGS% --help"
) else if /i "%~1"=="--debug" (
    set "PYTHON_ARGS=%PYTHON_ARGS% --mode debug"
) else if /i "%~1"=="--release" (
    set "PYTHON_ARGS=%PYTHON_ARGS% --mode release"
) else if /i "%~1"=="--wheel" (
    set "PYTHON_ARGS=%PYTHON_ARGS% --wheel"
) else if /i "%~1"=="--check" (
    set "PYTHON_ARGS=%PYTHON_ARGS% --check-only"
) else if /i "%~1"=="--troubleshoot" (
    set "PYTHON_ARGS=%PYTHON_ARGS% --troubleshoot"
) else (
    set "PYTHON_ARGS=%PYTHON_ARGS% %~1"
)
shift
goto :parse_args

:run_python
echo Running: python build_rust.py%PYTHON_ARGS%
echo.

REM Run the Python script
python build_rust.py%PYTHON_ARGS%
set "EXIT_CODE=%ERRORLEVEL%"

REM Return to original directory
popd

if %EXIT_CODE% neq 0 (
    echo.
    echo Build failed with exit code %EXIT_CODE%
    echo.
    echo Troubleshooting tips:
    echo - Make sure Rust is installed: https://rustup.rs/
    echo - Try running as Administrator
    echo - Check that Visual Studio Build Tools are installed
    echo - Run: %~nx0 --troubleshoot
    goto :error
)

echo.
echo Build completed successfully!
goto :end

:error
echo.
echo For more help, run: %~nx0 --help
echo For troubleshooting: %~nx0 --troubleshoot
exit /b 1

:end
exit /b 0