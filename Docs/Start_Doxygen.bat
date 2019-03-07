@echo off
rem Doxygen generates graphs using dot.exe (included with Graphviz)
rem UIMFLibrary_Doxy.ini defines "C:\Program Files (x86)\Graphviz\bin\dot.exe"

"C:\Program Files\doxygen\bin\doxygen.exe" UIMFLibrary_Doxy.ini

echo.
echo Press any key to create a .Zip file
pause

cd html

if exist "C:\Program Files\7-Zip\7z.exe" (goto SevenZip)

echo.
echo Compressing with Gnu zip (must be in your path)
zip -r ..\UIMFLibrary_ClassInfo.zip *
goto Done

:SevenZip
echo.
echo Compressing with 7-zip
"C:\Program Files\7-Zip\7z.exe" a -r ..\UIMFLibrary_ClassInfo.zip *
goto Done

:Done

cd ..

echo.
echo Moving the .zip file to ..\Releases\

if not exist ..\Releases mkdir ..\Releases
if exist UIMFLibrary_ClassInfo.zip (move /Y UIMFLibrary_ClassInfo.zip ..\Releases\) else echo Source file not found: UIMFLibrary_ClassInfo.zip
