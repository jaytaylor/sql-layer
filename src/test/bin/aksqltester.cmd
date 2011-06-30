@echo off
setlocal
for %%f in (%~dp0..\..\..\target\akiban-server-*.*.*-SNAPSHOT.jar) do set BASEJAR=%%~dpnf
java -cp %BASEJAR%-jar-with-dependencies.jar;%BASEJAR%-tests.jar com.akiban.sql.test.Tester %*
