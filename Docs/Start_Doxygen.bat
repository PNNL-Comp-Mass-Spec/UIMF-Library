@echo off
rem Doxygen generates graphs using dot.exe (included with Graphviz)
rem UIMFLibrary_Doxy.ini defines "C:\Program Files (x86)\Graphviz\bin\dot.exe"

"C:\Program Files\doxygen\bin\doxygen.exe" UIMFLibrary_Doxy.ini
