@echo off
setlocal
for %%f in (%~dp0..\..\..\target) do set TARGET=%%~dpnf
for %%f in (%TARGET%\akiban-server-*.*.*-SNAPSHOT.jar) do set BASEJAR=%%~dpnf
java -cp %BASEJAR%.jar;%BASEJAR%-tests.jar;%TARGET%\dependency\* com.akiban.sql.test.Tester %*
