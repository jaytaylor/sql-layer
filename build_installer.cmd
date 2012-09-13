@ECHO OFF

REM
REM END USER LICENSE AGREEMENT (“EULA”)
REM
REM READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
REM http://www.akiban.com/licensing/20110913
REM
REM BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
REM ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
REM AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
REM
REM IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
REM THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
REM NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
REM YOUR INITIAL PURCHASE.
REM
REM IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
REM CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
REM FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
REM LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
REM BY SUCH AUTHORIZED PERSONNEL.
REM
REM IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
REM USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
REM PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
REM

SETLOCAL

FOR /F "usebackq" %%v IN (`bzr revno`) DO SET BZR_REVNO=%%v

IF NOT DEFINED AKIBAN_CE_FLAG (
  SET TARGET=enterprise
  SET LICENSE=LICENSE-EE.txt
) ELSE (
  SET TARGET=developer
  SET LICENSE=LICENSE-DE.txt
)

IF NOT DEFINED CERT_FILE SET CERT_FILE=%~dp0\windows\testcert\testcert.pfx
IF NOT DEFINED CERT_PASSWORD SET CERT_PASSWORD=test

ECHO "Building Akiban Server for ##### %TARGET% #####"

call mvn -Dmaven.test.skip.exec clean install -DBZR_REVISION=%BZR_REVNO%
IF ERRORLEVEL 1 GOTO EOF

IF NOT DEFINED TOOLS_BRANCH SET TOOLS_BRANCH=lp:akiban-client-tools

CD target
bzr branch %TOOLS_BRANCH% client-tools
IF ERRORLEVEL 1 GOTO EOF
CD client-tools
call mvn -Dmaven.test.skip.exec clean install
IF ERRORLEVEL 1 GOTO EOF
DEL target\*-sources.jar
CD ..\..

MD target\isstage
MD target\isstage\bin
MD target\isstage\config
MD target\isstage\lib
MD target\isstage\procrun

COPY %LICENSE% target\isstage\LICENSE.TXT
XCOPY /E windows target\isstage
COPY bin\*.cmd target\isstage\bin
COPY target\client-tools\bin\*.cmd target\isstage\bin
COPY windows\%TARGET%\* target\isstage\config
COPY target\akiban-server-*-jar-with-dependencies.jar target\isstage\lib
COPY target\client-tools\target\akiban-client-tools-*.jar target\isstage\lib
COPY target\client-tools\target\dependency\*.jar target\isstage\lib

CD target\isstage

FOR %%j IN (lib\akiban-server-*.jar) DO SET JARFILE=%%~nj
FOR /F "delims=- tokens=3" %%n IN ("%JARFILE%") DO SET VERSION=%%n
SET INSTALLER=akiban-server-%VERSION%-installer

curl -o procrun.zip -L http://apache.spinellicreations.com/commons/daemon/binaries/windows/commons-daemon-1.0.10-bin-windows.zip
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
