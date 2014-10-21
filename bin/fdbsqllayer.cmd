@REM
@REM Copyright (C) 2009-2013 FoundationDB, LLC
@REM
@REM This program is free software: you can redistribute it and/or modify
@REM it under the terms of the GNU Affero General Public License as published by
@REM the Free Software Foundation, either version 3 of the License, or
@REM (at your option) any later version.
@REM
@REM This program is distributed in the hope that it will be useful,
@REM but WITHOUT ANY WARRANTY; without even the implied warranty of
@REM MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
@REM GNU Affero General Public License for more details.
@REM
@REM You should have received a copy of the GNU Affero General Public License
@REM along with this program.  If not, see <http://www.gnu.org/licenses/>.
@REM

@ECHO OFF

SETLOCAL EnableDelayedExpansion

SET SERVICE_NAME=fdbsqllayer
SET SERVICE_DNAME=FoundationDB SQL Layer 
SET SERVICE_DESC=FoundationDB SQL Layer

IF EXIST "%~dp0..\pom.xml" GOTO FROM_BUILD

REM Installation Configuration

FOR %%P IN ("%~dp0..") DO SET FDBSQL_HOME=%%~fP

CALL:findJarFile "%FDBSQL_HOME%\sql\lib"
SET DEP_DIR=%FDBSQL_HOME%\sql\lib\server
SET FDBSQL_HOME_DIR=%FDBSQL_HOME%\sql
SET FDBSQL_RFDIR=%FDBSQL_HOME%\sql\lib\routine-firewall
SET FDBSQL_POLICY=%FDBSQL_HOME%\sql\sql-layer-win.policy
@REM Replaced during install
SET FDBSQL_CONF=${confdir}
SET FDBSQL_LOGDIR=${logdir}

FOR %%P IN (prunsrv.exe) DO SET PRUNSRV=%%~$PATH:P
FOR %%P IN (prunmgr.exe) DO SET PRUNMGR=%%~$PATH:P
REM Not in path, assume installed with program.
IF "%PRUNSRV%"=="" (
  IF "%PROCESSOR_ARCHITECTURE%"=="x86" (
    SET PRUNSRV=%FDBSQL_HOME%\sql\procrun\prunsrv
  ) ELSE (
    SET PRUNSRV=%FDBSQL_HOME%\sql\procrun\%PROCESSOR_ARCHITECTURE%\prunsrv
) )
IF "%PRUNMGR%"=="" (
  SET PRUNMGR=%FDBSQL_HOME%\sql\procrun\prunmgr
)

GOTO PARSE_CMD

:FROM_BUILD

REM Build Configuration

FOR %%P IN ("%~dp0..") DO SET BUILD_HOME=%%~fP

CALL:findJarFile "%BUILD_HOME%\fdb-sql-layer-core\target"
SET DEP_DIR=%BUILD_HOME%\fdb-sql-layer-core\target\dependency
SET FDBSQL_CONF=%BUILD_HOME%\conf
SET FDBSQL_LOGDIR=\tmp\fdbsqllayer
SET FDBSQL_HOME_DIR=%BUILD_HOME%\fdb-sql-layer-core\target
SET FDBSQL_RFDIR=%BUILD_HOME%\routine-firewall\target
SET FDBSQL_POLICY=%BUILD_HOME%\sql-layer-win.policy
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
) ELSE IF "%1"=="-d" (
  SET DEP_DIR=%2
  SHIFT
  SHIFT
) ELSE IF "%1"=="-c" (
  SET FDBSQL_CONF=%2
  SHIFT
  SHIFT
) ELSE IF "%1"=="-l" (
  SET FDBSQL_LOGCONF=%2
  SHIFT
  SHIFT
) ELSE IF "%1"=="-g" (
  SET JVM_OPTS=%JVM_OPTS% -Dcom.persistit.showgui=true
  SHIFT
) ELSE IF "%1"=="-m" (
  SET SERVICE_MODE=%2
  SHIFT
  SHIFT
) ELSE IF "%1"=="-su" (
  SET SERVICE_USER=%2
  SET SERVICE_PASSWORD=%3
  SHIFT
  SHIFT
  SHIFT
) ELSE (
  GOTO USAGE
)

GOTO NEXT_OPT
:END_OPT

IF NOT EXIST "%JAR_FILE%" (
  ECHO JAR file does not exist; try -j
  GOTO EOF
)
IF NOT EXIST "%DEP_DIR%" (
  ECHO server dependencies directory does not exist; try -d
  GOTO EOF
)

SET CLASSPATH=%JAR_FILE%;%DEP_DIR%\*;%FDBSQL_RFDIR%\*

IF "%VERB%"=="version" GOTO VERSION

IF "%VERB%"=="start" (
  ECHO Starting service ...
  "%PRUNSRV%" //ES//%SERVICE_NAME%
  GOTO CHECK_ERROR
) ELSE IF "%VERB%"=="stop" (
  ECHO Stopping service ...
  "%PRUNSRV%" //SS//%SERVICE_NAME%
  GOTO CHECK_ERROR
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

IF NOT EXIST "%FDBSQL_CONF%\services-config.yaml" (
  ECHO Wrong configuration directory; try -c
  GOTO EOF
)

IF NOT DEFINED FDBSQL_LOGCONF SET FDBSQL_LOGCONF=%FDBSQL_CONF%\log4j.properties

IF EXIST "%FDBSQL_CONF%\jvm-options.cmd" CALL "%FDBSQL_CONF%\jvm-options.cmd"

IF "%VERB%"=="window" GOTO RUN_CMD
IF "%VERB%"=="run" GOTO RUN_CMD

SET PRUNSRV_ARGS=--StartMode=jvm ++StartParams="jvm" --StartClass com.foundationdb.sql.Main --StartMethod=procrunStart ^
                 --StopMode=jvm ++StopParams="jvm" --StopClass=com.foundationdb.sql.Main --StopMethod=procrunStop ^
                 --StdOutput="%FDBSQL_LOGDIR%\stdout.log" --DisplayName="%SERVICE_DNAME%" ^
                 --Description="%SERVICE_DESC%" --Startup=%SERVICE_MODE% --Classpath="%CLASSPATH%"
REM Each value that might have a space needs a separate ++JvmOptions.
SET PRUNSRV_ARGS=%PRUNSRV_ARGS% --JvmOptions="%JVM_OPTS: =#%" ++JvmOptions=-Xrs ++JvmOptions="-Dfdbsql.config_dir=%FDBSQL_CONF%" ^
                 ++JvmOptions="-Dlog4j.configuration=file:%FDBSQL_LOGCONF%" ++JvmOptions="-Dfdbsql.home=%FDBSQL_HOME_DIR%" ^
                 ++JvmOptions="-Djava.security.manager" ++JvmOptions="-Djava.security.policy=%FDBSQL_POLICY%"

IF DEFINED SERVICE_USER SET PRUNSRV_ARGS=%PRUNSRV_ARGS% --ServiceUser=%SERVICE_USER% --ServicePassword=%SERVICE_PASSWORD%
REM Important: JvmMs and JvmMx are in MB and do not accept unit suffix
IF DEFINED MAX_HEAP_MB SET PRUNSRV_ARGS=%PRUNSRV_ARGS% --JvmMs=%MAX_HEAP_MB% --JvmMx=%MAX_HEAP_MB%

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
FOR /F "usebackq" %%V IN (`java -cp "%CLASSPATH%" com.foundationdb.server.GetVersion`) DO SET SERVER_VERSION=%%V
FOR /F "usebackq" %%V IN (`java -cp "%CLASSPATH%" com.persistit.GetVersion`) DO SET PERSISTIT_VERSION=%%V
ECHO server   : %SERVER_VERSION%
ECHO persistit: %PERSISTIT_VERSION%
ECHO.
"%PRUNSRV%" //VS
GOTO EOF

:RUN_CMD
SET JVM_OPTS=%JVM_OPTS% -Dfdbsql.config_dir="%FDBSQL_CONF%"
SET JVM_OPTS=%JVM_OPTS% -Dlog4j.configuration="file:%FDBSQL_LOGCONF%"
SET JVM_OPTS=%JVM_OPTS% -ea
SET JVM_OPTS=%JVM_OPTS% -Dfdbsql.home="%FDBSQL_HOME_DIR%"
SET JVM_OPTS=%JVM_OPTS% -Djava.security.manager -Djava.security.policy="%FDBSQL_POLICY%"
IF DEFINED MAX_HEAP_MB SET JVM_OPTS=%JVM_OPTS% -Xms%MAX_HEAP_MB%M -Xmx%MAX_HEAP_MB%M
IF "%VERB%"=="window" GOTO WINDOW_CMD
java %JVM_OPTS% -cp "%CLASSPATH%" com.foundationdb.sql.Main
GOTO EOF

:WINDOW_CMD
SET JVM_OPTS=%JVM_OPTS% "-Drequire:com.foundationdb.sql.ui.SwingConsoleService" "-Dprioritize:com.foundationdb.sql.ui.SwingConsoleService" "-Dfdbsql.std_to_log=false"
START javaw %JVM_OPTS% -cp "%CLASSPATH%" com.foundationdb.sql.ui.MainWithSwingConsole
GOTO EOF

:CHECK_ERROR
IF ERRORLEVEL 1 GOTO PAUSE
GOTO EOF

:PAUSE
ECHO There was an error. Please check %FDBSQL_LOGDIR% for more information.
PAUSE
GOTO EOF

:findJarFile
SET JAR_FILE=
FOR %%P IN ("%~1\fdb-sql-layer*.jar") DO (
    SET T=%%P
    REM Ignore files that change with -tests, -sources and -client removed
    IF "!T:tests=!"=="%%P" IF "!T:sources=!"=="%%P" IF "!T:client=!"=="%%P" (
        SET JAR_FILE=%%P
    )
)
IF "%JAR_FILE%"=="" (
    ECHO No jar file in %~1
)
GOTO:EOF

:EOF
ENDLOCAL
