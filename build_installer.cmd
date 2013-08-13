@REM
@REM Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

FOR /F "usebackq" %%v IN (`git rev-parse --short HEAD`) DO SET GIT_COUNT=%%v
FOR /F "usebackq" %%v IN (`git rev-list --merges HEAD --count`) DO SET GIT_HASH=%%v

SET LICENSE=LICENSE.txt

IF NOT DEFINED CERT_FILE SET CERT_FILE=%~dp0\windows\testcert\testcert.pfx
IF NOT DEFINED CERT_PASSWORD SET CERT_PASSWORD=test

ECHO "Building Akiban Server"

call mvn clean install -DGIT_COUNT=%GIT_COUNT% -DGIT_HASH=%GIT_HASH% -DskipTests=true
IF ERRORLEVEL 1 GOTO EOF

IF NOT DEFINED TOOLS_BRANCH SET TOOLS_BRANCH=lp:akiban-client-tools

CD target
bzr branch %TOOLS_BRANCH% client-tools
IF ERRORLEVEL 1 GOTO EOF
CD client-tools
call mvn -Dmaven.test.skip clean install
IF ERRORLEVEL 1 GOTO EOF
DEL target\*-sources.jar
CD ..

CD ..

MD target\isstage
MD target\isstage\bin
MD target\isstage\config
MD target\isstage\lib
MD target\isstage\lib\plugins
MD target\isstage\lib\server
MD target\isstage\lib\client
MD target\isstage\procrun

COPY %LICENSE% target\isstage\LICENSE.TXT
XCOPY /E windows target\isstage
COPY bin\*.cmd target\isstage\bin
COPY target\client-tools\bin\*.cmd target\isstage\bin
COPY windows\config-files\* target\isstage\config
ECHO -tests.jar >target\xclude
ECHO -sources.jar >>target\xclude
XCOPY target\akiban-server-*.jar target\isstage\lib /EXCLUDE:target\xclude
COPY target\dependency\* target\isstage\lib\server
XCOPY target\client-tools\target\akiban-client-tools-*.jar target\isstage\lib /EXCLUDE:target\xclude
COPY target\client-tools\target\dependency\* target\isstage\lib\client

CD target\isstage

FOR %%j IN (lib\akiban-server-*.jar) DO SET JARFILE=%%~nj
FOR /F "delims=- tokens=3" %%n IN ("%JARFILE%") DO SET VERSION=%%n
SET INSTALLER=akiban-server-%VERSION%-installer

curl -o procrun.zip -L http://archive.apache.org/dist/commons/daemon/binaries/windows/commons-daemon-1.0.11-bin-windows.zip

IF ERRORLEVEL 1 GOTO EOF
7z x -oprocrun procrun.zip
IF ERRORLEVEL 1 GOTO EOF
CD procrun
mt -manifest ..\prunsrv.manifest -outputresource:prunsrv.exe;1
IF ERRORLEVEL 1 GOTO EOF
mt -manifest ..\prunmgr.manifest -outputresource:prunmgr.exe;1
IF ERRORLEVEL 1 GOTO EOF
CD amd64
mt -manifest ..\..\prunsrv.manifest -outputresource:prunsrv.exe;1
IF ERRORLEVEL 1 GOTO EOF
CD ..\ia64
mt -manifest ..\..\prunsrv.manifest -outputresource:prunsrv.exe;1
IF ERRORLEVEL 1 GOTO EOF
CD ..\..

iscc /S"GoDaddy=signtool sign /f $q%CERT_FILE%$q  /p $q%CERT_PASSWORD%$q /t http://tsa.starfieldtech.com/ $f" /O.. /F"%INSTALLER%" /dVERSION=%VERSION% AkibanServer.iss
IF ERRORLEVEL 1 GOTO EOF

CD ..\..

:EOF
ENDLOCAL
