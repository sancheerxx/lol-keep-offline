@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo [1/2] 检查打包工具...
python -c "import PyInstaller" 2>nul || (
    echo     未安装 PyInstaller，正在安装...
    python -m pip install pyinstaller || goto :fail
)

echo [2/2] 开始打包...
python -m PyInstaller --onefile --noconsole --name LolKeepOffline ^
    --icon app.ico --add-data "app.ico;." --clean ^
    lol_keep_offline_gui.py || goto :fail

echo.
echo 打包完成：dist\LolKeepOffline.exe
pause
exit /b 0

:fail
echo.
echo 打包失败，请查看上面的错误信息。
pause
exit /b 1
