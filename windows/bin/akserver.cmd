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

SET SERVER_JAR=akiban-server-1.1.0-SNAPSHOT-jar-with-dependencies.jar
SET SERVICE_NAME=akserver
SET SERVICE_DNAME=Akiban Server
SET SERVICE_DESC=Akiban Database Server

IF EXIST "%~dp0..\..\pom.xml" GOTO FROM_BUILD

REM Installation Configuration

FOR %%P IN ("%~dp0..") DO SET AKIBAN_HOME=%%~fP

SET JAR_FILE=%AKIBAN_HOME%\lib\%SERVER_JAR%
SET AKIBAN_CONF=%AKIBAN_HOME%
SET AKIBAN_LOGDIR=%AKIBAN_HOME%\log

FOR %%P IN (prunsrv.exe) DO SET PROCRUN=%%~$PATH:P
REM Not in path, assume installed with program.
IF "%PROCRUN%"=="" (
  IF "%PROCESSOR_ARCHITECTURE%"=="x86" (
    SET PROCRUN=%AKIBAN_HOME%\procrun\prunsrv
  ) ELSE (
    SET PROCRUN=%AKIBAN_HOME%\procrun\%PROCESSOR_ARCHITECTURE%\prunsrv
) )

GOTO PARSE_CMD

:FROM_BUILD

REM Build Configuration

FOR %%P IN ("%~dp0..\..") DO SET BUILD_HOME=%%~fP

SET JAR_FILE=%BUILD_HOME%\target\%SERVER_JAR%
SET AKIBAN_CONF=%BUILD_HOME%\windows
SET AKIBAN_LOGDIR=%~d0\akiban\log
SET PROCRUN=prunsrv

:PARSE_CMD

IF NOT DEFINED JVM_OPTS SET JVM_OPTS=-Dummy=

IF "%1"=="" GOTO USAGE

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
) ELSE IF "%1"=="-l" (
  SET AKIBAN_LOGCONF=%3
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
  "%PROCRUN%" //VS
  GOTO EOF
)

IF "%VERB%"=="start" (
  "%PROCRUN%" //ES//%SERVICE_NAME%
  GOTO EOF
) ELSE IF "%VERB%"=="stop" (
  "%PROCRUN%" //SS//%SERVICE_NAME%
  GOTO EOF
) ELSE IF "%VERB%"=="uninstall" (
  "%PROCRUN%" //DS//%SERVICE_NAME%
  GOTO EOF
) ELSE IF "%VERB%"=="console" (
  "%PROCRUN%" //TS//%SERVICE_NAME%
  GOTO EOF
) ELSE IF "%VERB%"=="monitor" (
  "%PROCRUN:srv=mgr%" //MR//%SERVICE_NAME%
  GOTO EOF
)

IF NOT EXIST "%JAR_FILE%" (
  ECHO JAR file does not exist; try -j
  GOTO EOF
)
IF NOT EXIST "%AKIBAN_CONF%\config\services-config.yaml" (
  ECHO Wrong configuration directory; try -c
  GOTO EOF
)

IF NOT DEFINED AKIBAN_LOGCONF SET AKIBAN_LOGCONF=%AKIBAN_CONF%\config\log4j.properties

IF EXIST "%AKIBAN_CONF%\config\jvm-options.cmd" CALL "%AKIBAN_CONF%\config\jvm-options.cmd"

IF "%VERB%"=="run" GOTO RUN_CMD

SET PROCRUN_ARGS=--StartMode=jvm --StartClass com.akiban.server.AkServer --StartMethod=procrunStart --StopMode=jvm --StopClass=com.akiban.server.AkServer --StopMethod=procrunStop --StdOutput="%AKIBAN_LOGDIR%\stdout.log" --DisplayName="%SERVICE_DNAME%" --Description="%SERVICE_DESC%" --Startup=manual --Classpath="%JAR_FILE%"
REM Each value that might have a space needs a separate ++JvmOptions.
SET PROCRUN_ARGS=%PROCRUN_ARGS% --JvmOptions="%JVM_OPTS: =#%" ++JvmOptions="-Dakiban.admin=%AKIBAN_CONF%" ++JvmOptions="-Dservices.config=%AKIBAN_CONF%\config\services-config.yaml" ++JvmOptions="-Dlog4j.configuration=file:%AKIBAN_LOGCONF%"
IF DEFINED MAX_HEAP_SIZE SET PROCRUN_ARGS=%PROCRUN_ARGS% --JvmMs=%MAX_HEAP_SIZE% --JvmMx=%MAX_HEAP_SIZE%

IF "%VERB%"=="install" (
  "%PROCRUN%" //IS//%SERVICE_NAME% %PROCRUN_ARGS%
  GOTO EOF
)

:USAGE
ECHO Usage: {install,uninstall,start,stop,run,monitor,version} [-j jarfile] [-c confdir] [-l log4j.properties] [-g]
GOTO EOF

:RUN_CMD
SET JVM_OPTS=%JVM_OPTS% -Dakiban.admin="%AKIBAN_CONF%"
SET JVM_OPTS=%JVM_OPTS% -Dservices.config="%AKIBAN_CONF%\config\services-config.yaml"
SET JVM_OPTS=%JVM_OPTS% -Dlog4j.configuration="file:%AKIBAN_LOGCONF%"
SET JVM_OPTS=%JVM_OPTS% -ea
IF DEFINED MAX_HEAP_SIZE SET JVM_OPTS=%JVM_OPTS% -Xms%MAX_HEAP_SIZE%-Xmx%MAX_HEAP_SIZE%
java %JVM_OPTS% -jar "%JAR_FILE%"
GOTO EOF

:EOF
ENDLOCAL
