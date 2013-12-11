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

call mvn clean package -U -DskipTests=true
IF ERRORLEVEL 1 GOTO EOF

IF NOT DEFINED TOOLS_LOC SET TOOLS_LOC="git@github.com:FoundationDB/sql-layer-client-tools.git"

CD target
git clone %TOOLS_LOC% client-tools
IF ERRORLEVEL 1 GOTO EOF
CD client-tools
call mvn clean package -U -DskipTests=true
IF ERRORLEVEL 1 GOTO EOF
DEL target\*-sources.jar
CD ..

CD ..

MD target\isstage
MD target\isstage\bin
MD target\isstage\conf
MD target\isstage\lib
MD target\isstage\lib\plugins
MD target\isstage\lib\server
MD target\isstage\lib\client
MD target\isstage\procrun

COPY %TOP_DIR%\LICENSE.txt target\isstage\LICENSE-SQL_LAYER.txt
COPY %EXE_DIR%\..\conf\* target\isstage\conf
XCOPY /E %EXE_DIR% target\isstage
COPY bin\*.cmd target\isstage\bin
COPY target\client-tools\bin\*.cmd target\isstage\bin
DEL target\isstage\conf\jvm.options
dos2unix --verbose --u2d target\isstage\conf\* target\isstage\*.txt target\isstage\bin\*.cmd
ECHO -tests.jar >target\xclude
ECHO -sources.jar >>target\xclude
XCOPY target\fdb-sql-layer-*.jar target\isstage\lib /EXCLUDE:target\xclude
COPY target\dependency\* target\isstage\lib\server
XCOPY target\client-tools\target\fdb-sql-layer-client-tools-*.jar target\isstage\lib /EXCLUDE:target\xclude
COPY target\client-tools\target\dependency\* target\isstage\lib\client

CD target\isstage

curl -o procrun.zip -L http://archive.apache.org/dist/commons/daemon/binaries/windows/commons-daemon-1.0.15-bin-windows.zip

IF ERRORLEVEL 1 GOTO EOF
7z x -oprocrun procrun.zip
IF ERRORLEVEL 1 GOTO EOF
CD procrun
mt /nologo -manifest ..\prunsrv.manifest -outputresource:prunsrv.exe;1
IF ERRORLEVEL 1 GOTO EOF
mt /nologo -manifest ..\prunmgr.manifest -outputresource:prunmgr.exe;1
IF ERRORLEVEL 1 GOTO EOF
CD amd64
mt /nologo -manifest ..\..\prunsrv.manifest -outputresource:prunsrv.exe;1
IF ERRORLEVEL 1 GOTO EOF
CD ..\ia64
mt /nologo -manifest ..\..\prunsrv.manifest -outputresource:prunsrv.exe;1
IF ERRORLEVEL 1 GOTO EOF
CD ..\..

iscc /S"standard=signtool sign /f $q%CERT_FILE%$q  /p $q%CERT_PASSWORD%$q /t http://tsa.starfieldtech.com/ $f" ^
     /O.. /F"%INSTALLER%" /dVERSION=%LAYER_VERSION% /dVERSIONTEXT=%VERSION_TEXT% /dRELEASE=%RELEASE% fdb-sql-layer.iss
IF ERRORLEVEL 1 GOTO EOF

CD ..\..

:EOF
ENDLOCAL
