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

SET SERVER_JAR=akiban-server-1.3.0-SNAPSHOT-jar-with-dependencies.jar
SET SERVICE_NAME=akserver
SET SERVICE_DNAME=Akiban Server
SET SERVICE_DESC=Akiban Database Server

IF EXIST "%~dp0..\pom.xml" GOTO FROM_BUILD

REM Installation Configuration

FOR %%P IN ("%~dp0..") DO SET AKIBAN_HOME=%%~fP

SET JAR_FILE=%AKIBAN_HOME%\lib\%SERVER_JAR%
SET AKIBAN_CONF=%AKIBAN_HOME%
SET AKIBAN_LOGDIR=%AKIBAN_HOME%\log

FOR %%P IN (prunsrv.exe) DO SET PRUNSRV=%%~$PATH:P
FOR %%P IN (prunmgr.exe) DO SET PRUNMGR=%%~$PATH:P
REM Not in path, assume installed with program.
IF "%PRUNSRV%"=="" (
  IF "%PROCESSOR_ARCHITECTURE%"=="x86" (
    SET PRUNSRV=%AKIBAN_HOME%\procrun\prunsrv
  ) ELSE (
    SET PRUNSRV=%AKIBAN_HOME%\procrun\%PROCESSOR_ARCHITECTURE%\prunsrv
) )
IF "%PRUNMGR%"=="" (
  SET PRUNMGR=%AKIBAN_HOME%\procrun\prunmgr
)

GOTO PARSE_CMD

:FROM_BUILD

REM Build Configuration

FOR %%P IN ("%~dp0..") DO SET BUILD_HOME=%%~fP

SET JAR_FILE=%BUILD_HOME%\target\%SERVER_JAR%
SET AKIBAN_CONF=%BUILD_HOME%\conf
SET AKIBAN_LOGDIR=\tmp\akiban_server
SET PRUNSRV=prunsrv
SET PRUNMGR=prunmgr
SET SERVICE_MODE=manual

:PARSE_CMD

IF NOT DEFINED JVM_OPTS SET JVM_OPTS=-Dummy=

IF "%1"=="" GOTO USAGE

SET VERB=%1
SHIFT

:NEXT_OPT

IF "%1"=="" GOTO END_OPT

IF "%1"=="-j" (
  SET JAR_FILE=%2
  SHIFT
  SHIFT
) ELSE IF "%1"=="-c" (
  SET AKIBAN_CONF=%2
  SHIFT
  SHIFT
) ELSE IF "%1"=="-l" (
  SET AKIBAN_LOGCONF=%2
  SHIFT
  SHIFT
) ELSE IF "%1"=="-g" (
  SET JVM_OPTS=%JVM_OPTS% -Dcom.persistit.showgui=true
  SHIFT
) ELSE IF "%1"=="-m" (
  SET SERVICE_MODE=%2
  SHIFT
  SHIFT
) ELSE (
  GOTO USAGE
)

GOTO NEXT_OPT
:END_OPT

IF "%VERB%"=="version" GOTO VERSION

IF "%VERB%"=="start" (
  "%PRUNSRV%" //ES//%SERVICE_NAME%
  GOTO EOF
) ELSE IF "%VERB%"=="stop" (
  "%PRUNSRV%" //SS//%SERVICE_NAME%
  GOTO EOF
) ELSE IF "%VERB%"=="uninstall" (
  "%PRUNSRV%" //DS//%SERVICE_NAME%
  GOTO EOF
) ELSE IF "%VERB%"=="console" (
  "%PRUNSRV%" //TS//%SERVICE_NAME%
  GOTO EOF
) ELSE IF "%VERB%"=="monitor" (
  START "%SERVICE_DNAME%" "%PRUNMGR%" //MS//%SERVICE_NAME%
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

IF "%VERB%"=="window" GOTO RUN_CMD
IF "%VERB%"=="run" GOTO RUN_CMD

SET PRUNSRV_ARGS=--StartMode=jvm --StartClass com.akiban.server.AkServer --StartMethod=procrunStart --StopMode=jvm --StopClass=com.akiban.server.AkServer --StopMethod=procrunStop --StdOutput="%AKIBAN_LOGDIR%\stdout.log" --DisplayName="%SERVICE_DNAME%" --Description="%SERVICE_DESC%" --Startup=%SERVICE_MODE% --Classpath="%JAR_FILE%"
REM Each value that might have a space needs a separate ++JvmOptions.
SET PRUNSRV_ARGS=%PRUNSRV_ARGS% --JvmOptions="%JVM_OPTS: =#%" ++JvmOptions="-Dakiban.admin=%AKIBAN_CONF%" ++JvmOptions="-Dservices.config=%AKIBAN_CONF%\config\services-config.yaml" ++JvmOptions="-Dlog4j.configuration=file:%AKIBAN_LOGCONF%"
IF DEFINED MAX_HEAP_SIZE SET PRUNSRV_ARGS=%PRUNSRV_ARGS% --JvmMs=%MAX_HEAP_SIZE% --JvmMx=%MAX_HEAP_SIZE%

IF "%VERB%"=="install" (
  "%PRUNSRV%" //IS//%SERVICE_NAME% %PRUNSRV_ARGS%
  GOTO EOF
)

:USAGE
ECHO Usage: {install,uninstall,start,stop,run,window,monitor,version} [-j jarfile] [-c confdir] [-l log4j.properties] [-g] [-m manual,auto]
ECHO install   - install as service
ECHO uninstall - remove installed service
ECHO start     - start installed service
ECHO stop      - stop installed service
ECHO window    - run as ordinary application
ECHO run       - run in command window
ECHO monitor   - start tray icon service monitor
ECHO version   - print version and exit
GOTO EOF

:VERSION
FOR /F "usebackq" %%V IN (`java -cp "%JAR_FILE%" com.akiban.server.GetVersion`) DO SET SERVER_VERSION=%%V
FOR /F "usebackq" %%V IN (`java -cp "%JAR_FILE%" com.persistit.GetVersion`) DO SET PERSISTIT_VERSION=%%V
ECHO server   : %SERVER_VERSION%
ECHO persistit: %PERSISTIT_VERSION%
ECHO.
"%PRUNSRV%" //VS
GOTO EOF

:RUN_CMD
SET JVM_OPTS=%JVM_OPTS% -Dakiban.admin="%AKIBAN_CONF%"
SET JVM_OPTS=%JVM_OPTS% -Dservices.config="%AKIBAN_CONF%\config\services-config.yaml"
SET JVM_OPTS=%JVM_OPTS% -Dlog4j.configuration="file:%AKIBAN_LOGCONF%"
SET JVM_OPTS=%JVM_OPTS% -ea
IF DEFINED MAX_HEAP_SIZE SET JVM_OPTS=%JVM_OPTS% -Xms%MAX_HEAP_SIZE%-Xmx%MAX_HEAP_SIZE%
IF "%VERB%"=="window" GOTO WINDOW_CMD
java %JVM_OPTS% -jar "%JAR_FILE%"
GOTO EOF

:WINDOW_CMD
SET JVM_OPTS=%JVM_OPTS% "-Drequire:com.akiban.server.service.ui.SwingConsoleService"
javaw %JVM_OPTS% -cp "%JAR_FILE%" com.akiban.server.service.ui.AkServerWithSwingConsole
GOTO EOF

:EOF
ENDLOCAL
