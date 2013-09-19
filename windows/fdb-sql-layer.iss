#ifndef VERSION
#define VERSION '1.0-UNKNOWN'
#endif

[Setup]
OutputBaseFilename = fdb-sql-layer
AppName = FoundationDB SQL Layer
AppVerName = FoundationDB SQL Layer {#VERSION}
AppPublisher = FoundationDB, LCC
AppPublisherURL = https://foundationdb.com/
AppVersion = {code:JustVersion|{#VERSION}}
VersionInfoProductTextVersion = {#VERSION}
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
;SignTool=GoDaddy

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
Filename: "{app}\bin\fdbsqllayer.cmd"; Parameters: "start"; WorkingDir: "{app}"; StatusMsg: "Starting service ..."; Flags: runminimized

[UninstallRun]
Filename: "{app}\bin\fdbsqllayer.cmd"; Parameters: "stop";  WorkingDir: "{app}"; StatusMsg: "Stopping service ..."; Flags: runminimized
Filename: "{app}\bin\fdbsqllayer.cmd"; Parameters: "uninstall";  WorkingDir: "{app}"; StatusMsg: "Removing service ..."; Flags: runhidden

[Code]
function FindJava(): String;
var
  JavaLoc : String;
  JavaVersion : String;
  JavaInstalled : Boolean;
begin
  Result := '';
  JavaLoc := 'SOFTWARE\JavaSoft\Java Runtime Environment';
  JavaInstalled := RegKeyExists(HKLM, JavaLoc);

  if not JavaInstalled then begin
    JavaLoc := 'SOFTWARE\Wow6432Node\JavaSoft\Java Runtime Environment';
    JavaInstalled := RegKeyExists(HKLM, JavaLoc);
  end;

  if JavaInstalled then
    begin
      if RegQueryStringValue(HKLM, JavaLoc, 'CurrentVersion', JavaVersion) then
        begin
          if CompareStr('1.7', JavaVersion) > 0 then
            Result := 'Java version >= 1.7 is required to run FoundationDB SQL Layer';
        end
      else 
        Result := 'Unable to detect Java version';
    end
  else
    Result := 'Java is required to run FoundationDB SQL Layer';
end;

function FindFDBClient(): String;
var
  FDBClientVersion : Cardinal;
begin
  Result := '';
  if RegQueryDWordValue(HKLM, 'SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{66E3CE1B-CDE3-45E0-9B6F-50949D5F2411}', 'Version', FDBClientVersion) then
    begin
      if FDBClientVersion < $01000000 then
        Result := 'FoundationDB Client version >= 1.0 is required to run FoundationDB SQL Layer';
    end
  else
    Result := 'FoundationDB Client required to run FoundationDB SQL Layer';
end;

function InitializeSetup(): Boolean;
var
  ErrorMsg : String;
begin
  Result := true;
  ErrorMsg := FindJava()
  if Length(ErrorMsg) > 0 then begin
    MsgBox(ErrorMsg, mbError, MB_OK);
    Result := false;
  end
  ErrorMsg := FindFDBClient()
  if Length(ErrorMsg) > 0 then begin
    MsgBox(ErrorMsg, mbError, MB_OK);
    Result := false;
  end
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
    Value := ''
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

function JustVersion(Param: String): String;
var
  DashPos: Integer;
begin
  DashPos := Pos('-', Param);
  if (DashPos <> 0) then
    Result := Copy(Param, 1, DashPos - 1)
  else
    Result := Param;
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

