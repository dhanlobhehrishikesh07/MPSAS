@echo off
:: ============================================================
::  One Click Server – Windows Launcher
::  Double-click this file on ANY Windows PC to start the app.
::  Works even if the folder path contains spaces (OneDrive etc.)
:: ============================================================

:: Always cd to this file's folder first
cd /d "%~dp0"

echo.
echo  ================================
echo   ONE CLICK SERVER
echo   Starting up...
echo  ================================
echo.
echo  Folder: %~dp0
echo.

:: Try 'python' (Microsoft Store / added to PATH)
where python >nul 2>&1
if %errorlevel% == 0 (
    echo  Found: python
    python main.py
    goto done
)

:: Try 'py' launcher (python.org installer)
where py >nul 2>&1
if %errorlevel% == 0 (
    echo  Found: py
    py main.py
    goto done
)

:: Try common install paths
for %%P in (
    "C:\Python312\python.exe"
    "C:\Python311\python.exe"
    "C:\Python310\python.exe"
    "C:\Python39\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python310\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python39\python.exe"
) do (
    if exist %%P (
        echo  Found Python at: %%P
        %%P main.py
        goto done
    )
)

:: Not found
echo.
echo  ERROR: Python not found on this PC.
echo.
echo  Please install Python 3.9 or newer from:
echo    https://www.python.org/downloads/
echo.
echo  IMPORTANT: Tick "Add Python to PATH" during install!
echo.
pause
goto end

:done
if %errorlevel% neq 0 (
    echo.
    echo  App exited with error code %errorlevel%
    echo  Check logs\server_debug.log for details.
    pause
)

:end
