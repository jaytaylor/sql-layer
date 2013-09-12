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

@echo off
setlocal
for %%f in (%~dp0..\..\..\target) do set TARGET=%%~dpnf
for %%f in (%TARGET%\fdb-sql-layer-*.*.*-SNAPSHOT.jar) do set BASEJAR=%%~dpnf
java -cp %BASEJAR%.jar;%BASEJAR%-tests.jar;%TARGET%\dependency\* com.foundationdb.sql.test.Tester %*
