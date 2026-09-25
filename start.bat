@echo off
title 可转债成交额监控
cd /d "%~dp0"

echo ============================================
echo   可转债成交额监控 - 一键启动
echo ============================================
echo.

REM ---- 1. 检查服务是否已在运行（端口 8011）----
netstat -ano | findstr ":8011" | findstr "LISTENING" >nul 2>nul
if %errorlevel%==0 (
    echo [信息] 服务已在运行 ^(端口 8011^)
    start "" "http://127.0.0.1:8011"
    echo [信息] 已在浏览器打开监控页面
    echo.
    pause
    exit /b 0
)

REM ---- 2. 定位 Python 解释器 ----
set "PY="
if exist "C:\Users\jones\.workbuddy\binaries\python\versions\3.13.12\python.exe" (
    set "PY=C:\Users\jones\.workbuddy\binaries\python\versions\3.13.12\python.exe"
) else (
    where python >nul 2>nul
    if %errorlevel%==0 set "PY=python"
)
if not defined PY (
    echo [错误] 未找到 Python 解释器，请先安装 Python 3.9 或更高版本
    echo.
    pause
    exit /b 1
)

echo [信息] 使用 Python: %PY%
echo [信息] 正在启动行情监控服务...
echo [信息] 约 6 秒后自动打开浏览器页面
echo.
echo [提示] 请勿关闭本窗口，关闭窗口即停止服务
echo.

REM ---- 3. 延迟打开浏览器，等待服务就绪 ----
start "" /min cmd /c "ping -n 7 127.0.0.1 >nul & explorer http://127.0.0.1:8011"

REM ---- 4. 前台运行服务 ----
"%PY%" -u "%~dp0app.py"

echo.
echo [信息] 服务已停止
pause
