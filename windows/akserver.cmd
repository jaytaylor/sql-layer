@ECHO OFF

REM Copyright (C) 2012 Akiban Technologies Inc.
REM This program is free software: you can redistribute it and/or modify
REM it under the terms of the GNU Affero General Public License, version 3,
REM as published by the Free Software Foundation.

REM This program is distributed in the hope that it will be useful,
REM but WITHOUT ANY WARRANTY; without even the implied warranty of
REM MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
REM GNU Affero General Public License for more details.
REM
REM You should have received a copy of the GNU Affero General Public License
REM along with this program.  If not, see http://www.gnu.org/licenses.

SETLOCAL

REM Defaults
FOR %%P IN (%~dp0..) DO SET AKIBAN_HOME=%%~fP
SET JAR_FILE=%AKIBAN_HOME%\target\akiban-server-1.1.0-SNAPSHOT-jar-with-dependencies.jar
SET AKIBAN_CONF=%AKIBAN_HOME%\conf
SET SERVICE_NAME=akserver
SET SERVICE_DNAME=Akiban Server
SET SERVICE_DESC=Akiban Database Server

FOR %%P IN (prunsrv.exe) DO SET PROCRUN=%%~$PATH:P
REM Not in path, assume installed with program.
IF "%PROCRUN%"=="" (
  IF "%PROCESSOR_ARCHITECTURE%"=="x86" (
    SET PROCRUN=%AKIBAN_HOME%\bin\prunsrv
  ) ELSE (
    SET PROCRUN=%AKIBAN_HOME%\bin\%PROCESSOR_ARCHITECTURE%\prunsrv
) )

IF NOT DEFINED JVM_OPTS SET JVM_OPTS=-Dummy=

SET VERB=%1
SHIFT

:NEXT_OPT

IF "%1"=="" GOTO END_OPT

IF "%1"=="-j" (
  SET JAR_FILE=%3
  SHIFT
  SHIFT
) ELSE IF "%1"=="-c" (
  SET AKIBAN_CONF=%3
  SHIFT
  SHIFT
) ELSE IF "%1"=="-g" (
  SET JVM_OPTS=%JVM_OPTS% -Dcom.persistit.showgui=true
  SHIFT
) ELSE (
  GOTO USAGE
)

GOTO NEXT_OPT
:END_OPT

IF "%VERB%"=="version" (
  FOR /F "usebackq" %%V IN (`java -cp "%JAR_FILE%" com.akiban.server.GetVersion`) DO ECHO server   : %%V
  FOR /F "usebackq" %%V IN (`java -cp "%JAR_FILE%" com.persistit.GetVersion`) DO ECHO persistit: %%V
  %PROCRUN% //VS
  GOTO EOF
)

IF "%VERB%"=="start" (
  %PROCRUN% //ES//%SERVICE_NAME%
  GOTO EOF
) ELSE IF "%VERB%"=="stop" (
  %PROCRUN% //SS//%SERVICE_NAME%
  GOTO EOF
) ELSE IF "%VERB%"=="uninstall" (
  %PROCRUN% //DS//%SERVICE_NAME%
  GOTO EOF
) ELSE IF "%VERB%"=="console" (
  %PROCRUN% //TS//%SERVICE_NAME%
  GOTO EOF
) ELSE IF "%VERB%"=="monitor" (
  %PROCRUN:srv=mgr% //MR//%SERVICE_NAME%
  GOTO EOF
)

SET JVM_OPTS=%JVM_OPTS% -Dakiban.admin=%AKIBAN_CONF%
SET JVM_OPTS=%JVM_OPTS% -Dservices.config=%AKIBAN_CONF%\config\services-config.yaml
IF EXIST %AKIBAN_CONF%\jvm-options.cmd CALL %AKIBAN_CONF%\jvm-options.cmd

IF "%VERB%"=="run" GOTO RUN_CMD

SET JVM_OPTS=%JVM_OPTS% -Dlog4j.configuration=file:%AKIBAN_CONF%\config\log4j.properties

SET JVM_HASH_OPTS=%JVM_OPTS: =#%

SET PROCRUN_ARGS=--StartMode=jvm --StartClass com.akiban.server.AkServer --StartMethod=procrunStart --StopMode=jvm --StopClass=com.akiban.server.AkServer --StopMethod=procrunStop --StdOutput="%AKIBAN_HOME%\log\stdout.log" --DisplayName="%SERVICE_DNAME%" --Description="%SERVICE_DESC%" --Startup=manual --Classpath="%JAR_FILE%" --JvmOptions="%JVM_HASH_OPTS%"
IF DEFINED MAX_HEAP_SIZE SET PROCRUN_ARGS=%PROCRUN_ARGS% --JvmMs=%MAX_HEAP_SIZE% --JvmMx=%MAX_HEAP_SIZE%

IF "%VERB%"=="install" (
  %PROCRUN% //IS//%SERVICE_NAME% %PROCRUN_ARGS%
  GOTO EOF
)

:USAGE
ECHO Usage: {install,uninstall,start,stop,run,monitor,version} [-j jarfile] [-c confdir] [-g]

:RUN_CMD
SET JVM_OPTS=%JVM_OPTS% -ea
IF DEFINED MAX_HEAP_SIZE SET JVM_OPTS=%JVM_OPTS% -Xms%MAX_HEAP_SIZE%-Xmx%MAX_HEAP_SIZE%
java %JVM_OPTS% -jar "%JAR_FILE%"
GOTO EOF

:EOF
ENDLOCAL
