#ifndef VERSION
#define VERSION '0.0.0'
#endif

#ifndef VERSIONFULL
#define VERSIONFULL '0.0.0-UNKNOWN'
#endif

#define APPNAME 'FoundationDB SQL Layer'
#define FDB_UPGRADE_GUID '{A95EA002-686E-4164-8356-C715B7F8B1C8}'
#define FDB_VERSION_STR '1.0.1'
#define FDB_VERSION_NUM '$01000001'


[Setup]
OutputBaseFilename = fdb-sql-layer
AppName = {#APPNAME}
AppVerName = {#APPNAME} {#VERSION}
AppPublisher = FoundationDB, LCC
AppPublisherURL = https://foundationdb.com/
AppVersion = {#VERSION}
AppCopyright = Copyright (c) 2009-2013 FoundationDB, LLC
VersionInfoVersion = {#VERSION}
VersionInfoProductTextVersion = {#VERSIONFULL}
DefaultDirName = {code:DefaultInstallPath}
DefaultGroupName = foundationdb
Compression = lzma
SolidCompression = yes
MinVersion = 5.1
PrivilegesRequired = admin
ArchitecturesInstallIn64BitMode=x64 ia64
LicenseFile=LICENSE-SQL_LAYER.txt
DirExistsWarning = no
SetupIconFile = foundationdb.ico
WizardImageFile = dialog.bmp
WizardSmallImageFile = banner.bmp
WizardImageStretch = no
WizardImageBackColor = clWhite
InfoAfterFile = conclusion.rtf
SignTool=standard

[Tasks]
;Always run as Service, SYSTEM user, and start with windows

[Dirs]
Name: "{code:AppDataDir}\logs\sql"
Name: "{code:AppDataDir}\sql-config"

[Files]
Source: "LICENSE-SQL_LAYER.txt"; DestDir: "{app}"
Source: "bin\*"; DestDir: "{app}\bin"; AfterInstall: EditAfterInstall
Source: "config\*"; DestDir: "{code:AppDataDir}\sql-config"; AfterInstall: EditAfterInstall; Flags: onlyifdoesntexist uninsneveruninstall
Source: "lib\*"; DestDir: "{app}\sql\lib"; Flags: recursesubdirs
Source: "procrun\*"; DestDir: "{app}\sql\procrun"; Flags: recursesubdirs

[Icons]
;Name: "{group}\FoundationDB SQL Layer"; Filename: "{app}\bin\fdbsqllayer.cmd"; Parameters: "window";  WorkingDir: "{app}"; Comment: "Run the server as an application"
;Name: "{group}\Start Service"; Filename: "{app}\bin\fdbsqllayer.cmd"; Parameters: "start";  WorkingDir: "{app}"; Comment: "Start Windows service"
;Name: "{group}\Stop Service"; Filename: "{app}\bin\fdbsqllayer.cmd"; Parameters: "stop";  WorkingDir: "{app}"; Comment: "Stop Windows service"
;Name: "{group}\Monitor Service"; Filename: "{app}\bin\fdbsqllayer.cmd"; Parameters: "monitor";  WorkingDir: "{app}"; Comment: "Monitor Windows service"
;Name: "{group}\Uninstall FoundationDB SQL Layer"; Filename: "{uninstallexe}"

[Run]
Filename: "{app}\bin\fdbsqllayer.cmd"; Parameters: "install -m auto"; WorkingDir: "{app}"; StatusMsg: "Installing service ..."; Flags: runhidden
Filename: "{app}\bin\fdbsqllayer.cmd"; Parameters: "start"; WorkingDir: "{app}"; StatusMsg: "Starting service ..."

[UninstallRun]
Filename: "{app}\bin\fdbsqllayer.cmd"; Parameters: "uninstall";  WorkingDir: "{app}"; StatusMsg: "Removing service ..."; Flags: runhidden

[Code]
//
// Custom functions
//
#ifdef UNICODE
  #define AW "W"
#else
  #define AW "A"
#endif

function MsiEnumRelatedProducts(lpUpgradeCode: String; dwReserved: DWORD; iProductIndex: DWORD; lpProductBuf: String): UINT;
  external 'MsiEnumRelatedProducts{#AW}@msi.dll stdcall';

function FindJava(): String;
var
  JavaLoc : String;
  JavaVersion : String;
  JavaInstalled : Boolean;
begin
  Result := 'Java version >= 7 is required to run {#APPNAME}.';

  // If not found at this location, could be x64 with x86 Java. Not supported by fdb-java.
  JavaInstalled := RegKeyExists(HKLM, 'SOFTWARE\JavaSoft\Java Runtime Environment');
  if not JavaInstalled then begin
    if RegKeyExists(HKLM, 'SOFTWARE\Wow6432Node\JavaSoft\Java Runtime Environment') then
      Result := 'Detected 32-bit Java install. Not supported on 64-bit Windows.';
  else
    Result := 'No Java found. ' + Result;

  if JavaInstalled then
    begin
      if RegQueryStringValue(HKLM, JavaLoc, 'CurrentVersion', JavaVersion) then
        begin
          if CompareStr('1.7', JavaVersion) <= 0 then
            Result := ''; // Success
        end
      else 
        Result := 'Unable to detect Java version';
    end
end;

function FindFDBClient(): String;
var
  I : Integer;
  ProductGUID : String;
  FDBClientVersion : Cardinal;
begin
  I := 0;
  SetLength(ProductGUID, 39);

  while MsiEnumRelatedProducts('{#FDB_UPGRADE_GUID}', 0, I, ProductGUID) = 0 do
  begin
    SetLength(ProductGUID, 39);
    I := I + 1;
  end;

  Result := '';
  if I = 0 then
    Result := 'No FoundationDB found. Version >= {#FDB_VERSION_STR} required to run {#APPNAME}'
  else if I > 1 then
    Result := 'Error: Multiple FoundationDB installations found.'
  else
    begin
      if RegQueryDWordValue(HKLM, 'SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\' + ProductGUID, 'Version', FDBClientVersion) then
        begin
          if FDBClientVersion < {#FDB_VERSION_NUM} then
            Result := 'FoundationDB Client version >= {#FDB_VERSION_STR} required to run {#APPNAME}';
        end
      else
        Result := 'Error: No such product ' + ProductGUID;
    end;
end;

function ExpandKey(Key: String) : String;
var
  Value: String;
begin
  if Key = 'datadir' then
    Value := '{code:AppDataDir}\data\sql'
  else if Key = 'confdir' then
    Value := '{code:AppDataDir}\sql-config'
  else if Key = 'logdir' then
    Value := '{code:AppDataDir}\logs\sql'
  else if Key = 'tempdir' then
    Value := '{%TEMP}'
  else
    Value := '';
  Value := ExpandConstant(Value);
  StringChange(Value, '\', '/');
  Result := Value;
end;

procedure EditFile(FileName: String);
var
  FileLines: TArrayOfString;
  Index, DollarBrace, EndBrace: Integer;
  Key: String;
begin
  if FileExists(FileName) then begin
    LoadStringsFromFile(FileName, FileLines);
    for Index := 0 to GetArrayLength(FileLines) - 1 do begin
      DollarBrace := Pos('${', FileLines[Index]);
      if DollarBrace <> 0 then begin
        EndBrace := Pos('}', FileLines[Index]);
        Key := Copy(FileLines[Index], DollarBrace + 2, EndBrace - DollarBrace - 2);
        Delete(FileLines[Index], DollarBrace, EndBrace - DollarBrace + 1);
        Insert(ExpandKey(Key), FileLines[Index], DollarBrace);
      end
    end;
    SaveStringsToFile(FileName, FileLines, false);
  end;
end;

procedure EditAfterInstall();
var
  Path: String;
  FileName: String;
begin
  Path := ExpandConstant(CurrentFileName);
  FileName := ExtractFileName(Path)
  if (FileName = 'fdbsqllayer.cmd') or (FileName = 'log4j.properties') or (FileName = 'server.properties') then
    EditFile(Path)
end;

function AppDataDir(Param: String): String;
begin
  Result := ExpandConstant('{commonappdata}\foundationdb')
end;

function DefaultInstallPath(Param: String): String;
begin;
  Result := GetEnv('FOUNDATIONDB_INSTALL_PATH');
  if Length(Result) = 0 then
    Result := ExpandConstant('{pf}\foundationdb')
end;


//
// Built-In Events
//

function InitializeSetup(): Boolean;
var
  ErrorMsg : String;
begin
  Result := true;
  ErrorMsg := FindJava()
  if Length(ErrorMsg) > 0 then begin
    MsgBox(ErrorMsg, mbError, MB_OK);
    Result := false;
  end;
  ErrorMsg := FindFDBClient();
  if Length(ErrorMsg) > 0 then begin
    MsgBox(ErrorMsg, mbError, MB_OK);
    Result := false;
  end
end;

function PrepareToInstall(var NeedsRestart: Boolean): String;
var
  UninstallKey : String;
  InstallPath : String;
  CmdPath : String;
  ResultCode : Integer;
begin
  UninstallKey := ExpandConstant('Software\Microsoft\Windows\CurrentVersion\Uninstall\{#APPNAME}_is1');
  if RegQueryStringValue(HKLM, UninstallKey, 'InstallLocation', InstallPath) then begin
    CmdPath := InstallPath + '\bin\fdbsqllayer.cmd';
    if FileExists(CmdPath) then
      Exec(CmdPath, 'uninstall', InstallPath, SW_HIDE, ewWaitUntilTerminated, ResultCode);
  end;
  Result := '';
end;

