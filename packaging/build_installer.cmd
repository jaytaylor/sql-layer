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

SETLOCAL

IF "%1"=="" (
  SET RELEASE=0
) ELSE IF "%1"=="-r" (
  SET RELEASE=1
) ELSE (
  ECHO Unexpected argument: %1
  EXIT /B 1
)


REM Program is named hd2u in msys-git 1.9 and dos2unix in version prior

WHERE hd2u >NUL 2>&1
IF "%ERRORLEVEL%"=="0" (
  SET DOS2UNIX=hd2u
)
WHERE dos2unix >NUL 2>&1
IF "%ERRORLEVEL%"=="0" (
  SET DOS2UNIX=dos2unix
)
IF "%DOS2UNIX%"=="" (
    ECHO No hd2u or dos2unix found
    EXIT /B 1
)


SET EXE_DIR=%~dp0\exe
SET TOP_DIR=%EXE_DIR%\..\..
CD %TOP_DIR%

FOR /F "usebackq" %%v IN (`powershell -Command "& {[xml]$p=Get-Content pom.xml ; $p.project.version}"`) DO SET LAYER_MVN_VERSION=%%v
FOR /F "usebackq" %%v IN (`git rev-parse --short HEAD`) DO SET GIT_HASH=%%v
SET LAYER_VERSION=%LAYER_MVN_VERSION:-SNAPSHOT=%
SET VERSION_TEXT=%LAYER_MVN_VERSION%.%RELEASE%-%GIT_HASH%
SET INSTALLER=fdb-sql-layer-%LAYER_VERSION%-%RELEASE%

IF NOT DEFINED CERT_FILE SET CERT_FILE=%EXE_DIR%\testcert\testcert.pfx
IF NOT DEFINED CERT_PASSWORD SET CERT_PASSWORD=test

ECHO "Building FoundationDB SQL Layer %LAYER_VERSION% Release %RELEASE%"

call mvn clean package -U -D"fdbsql.release=%RELEASE%" -D"skipTests=true"
IF ERRORLEVEL 1 GOTO EOF

IF NOT DEFINED TOOLS_LOC SET TOOLS_LOC="git@github.com:FoundationDB/sql-layer-client-tools.git"
IF NOT DEFINED TOOLS_REF SET TOOLS_REF="master"

CD fdb-sql-layer-core/target
git clone %TOOLS_LOC% client-tools
IF ERRORLEVEL 1 GOTO EOF
CD client-tools
git checkout -b scratch %TOOLS_REF%
IF ERRORLEVEL 1 GOTO EOF
call mvn clean package -U -D"fdbsql.release=%RELEASE%" -D"skipTests=true"
IF ERRORLEVEL 1 GOTO EOF
DEL target\*-sources.jar
CD ..
CD ..
CD ..

REM Common files
MD target
MD target\isstage

XCOPY /E %EXE_DIR% target\isstage
ECHO -tests.jar > target\xclude
ECHO -sources.jar >> target\xclude

REM SQL Layer component files
MD target\isstage\layer
MOVE target\isstage\conf target\isstage\layer
MD target\isstage\layer\bin
MD target\isstage\layer\lib
MD target\isstage\layer\lib\plugins
MD target\isstage\layer\lib\server
MD target\isstage\layer\lib\routine-firewall
MD target\isstage\layer\procrun

COPY %TOP_DIR%\LICENSE.txt target\isstage\layer\LICENSE-SQL_LAYER.txt
COPY %EXE_DIR%\..\conf\* target\isstage\layer\conf
DEL target\isstage\layer\conf\jvm.options
COPY bin\*.cmd target\isstage\layer\bin
%DOS2UNIX% --verbose --u2d target\isstage\layer\conf\* target\isstage\layer\*.txt target\isstage\layer\bin\*.cmd
FOR %%f in (target\isstage\layer\conf\*) DO MOVE "%%f" "%%f.new"
XCOPY fdb-sql-layer-core\target\fdb-sql-layer-*.jar target\isstage\layer\lib /EXCLUDE:target\xclude
XCOPY fdb-sql-layer-core\target\dependency\* target\isstage\layer\lib\server
XCOPY routine-firewall\target\routine-firewall*.jar target\isstage\lib\routine-firewall\ /EXCLUDE:target\xclude
COPY %TOP_DIR%\sql-layer.policy target\isstage\lib
COPY %TOP_DIR%\sql-layer-win.policy target\isstage\lib

CD target\isstage\layer
curl -o procrun.zip -L http://archive.apache.org/dist/commons/daemon/binaries/windows/commons-daemon-1.0.15-bin-windows.zip

IF ERRORLEVEL 1 GOTO EOF
7z x -oprocrun procrun.zip
IF ERRORLEVEL 1 GOTO EOF
CD procrun
mt /nologo -manifest ..\..\prunsrv.manifest -outputresource:prunsrv.exe;1
IF ERRORLEVEL 1 GOTO EOF
mt /nologo -manifest ..\..\prunmgr.manifest -outputresource:prunmgr.exe;1
IF ERRORLEVEL 1 GOTO EOF
CD amd64
mt /nologo -manifest ..\..\..\prunsrv.manifest -outputresource:prunsrv.exe;1
IF ERRORLEVEL 1 GOTO EOF
CD ..\ia64
mt /nologo -manifest ..\..\..\prunsrv.manifest -outputresource:prunsrv.exe;1
IF ERRORLEVEL 1 GOTO EOF

cd %TOP_DIR%

REM Client Tools component files
MD target\isstage\client
MD target\isstage\client\bin
MD target\isstage\client\lib
MD target\isstage\client\lib\client

COPY fdb-sql-layer-core\target\client-tools\bin\*.cmd target\isstage\client\bin
XCOPY fdb-sql-layer-core\target\client-tools\target\fdb-sql-layer-client-tools-*.jar target\isstage\client\lib /EXCLUDE:target\xclude
COPY fdb-sql-layer-core\target\client-tools\target\dependency\* target\isstage\client\lib\client
COPY fdb-sql-layer-core\target\client-tools\LICENSE.txt target\isstage\client\LICENSE-SQL_LAYER_CLIENT_TOOLS.txt

REM Build the installer
CD target\isstage

iscc /S"standard=signtool sign /f $q%CERT_FILE%$q  /p $q%CERT_PASSWORD%$q /t http://tsa.starfieldtech.com/ $f" ^
     /O.. /F"%INSTALLER%" /dVERSION=%LAYER_VERSION% /dVERSIONTEXT=%VERSION_TEXT% /dRELEASE=%RELEASE% fdb-sql-layer.iss
IF ERRORLEVEL 1 GOTO EOF

CD ..\..

:EOF
ENDLOCAL
