#ifndef VERSION
#define VERSION '1.0-UNKNOWN'
#endif

[Setup]
OutputBaseFilename = AkibanServer
AppName = Akiban Server
AppVerName = Akiban Server {#VERSION}
AppPublisher = Akiban Technologies
AppPublisherURL = http://www.akiban.com/
AppVersion = {code:JustVersion|{#VERSION}}
VersionInfoProductTextVersion = {#VERSION}
DefaultDirName = {pf}\Akiban
DefaultGroupName = Akiban
Compression = lzma
SolidCompression = yes
MinVersion = 5.1
PrivilegesRequired = admin
ArchitecturesInstallIn64BitMode=x64 ia64

[Tasks]
Name: "installsvc"; Description: "Run as Windows Service"; Flags: unchecked
Name: "installsvc\auto"; Description: "Start with Windows"
Name: "start"; Description: "Start now"

[Dirs]
Name: "{commonappdata}\Akiban\data"
Name: "{commonappdata}\Akiban\log"

[Files]
Source: "LICENSE.txt"; DestDir: "{app}"
Source: "bin\*"; DestDir: "{app}\bin"
Source: "config\*"; DestDir: "{app}\config"
Source: "lib\*"; DestDir: "{app}\lib"
Source: "procrun\*"; DestDir: "{app}\procrun"; Flags: recursesubdirs

[Icons]
Name: "{group}\Akiban Server"; Filename: "{app}\bin\akserver.cmd"; Parameters: "window";  WorkingDir: "{app}"; Comment: "Run the server as an application"; IconFilename: "{app}\bin\Akiban_Server.ico"
Name: "{group}\Start Service"; Filename: "{app}\bin\akserver.cmd"; Parameters: "start";  WorkingDir: "{app}"; Comment: "Start Windows service"
Name: "{group}\Stop Service"; Filename: "{app}\bin\akserver.cmd"; Parameters: "stop";  WorkingDir: "{app}"; Comment: "Stop Windows service"
Name: "{group}\Monitor Service"; Filename: "{app}\bin\akserver.cmd"; Parameters: "monitor";  WorkingDir: "{app}"; Comment: "Monitor Windows service"

[Run]
Filename: "{app}\bin\akserver.cmd"; Parameters: "install -m {code:InstallMode}";  WorkingDir: "{app}"; StatusMsg: "Installing service ..."; Tasks: installsvc
Filename: "{app}\bin\akserver.cmd"; Parameters: "start";  WorkingDir: "{app}"; StatusMsg: "Starting service ..."; Tasks: start and installsvc
Filename: "{app}\bin\akserver.cmd"; Parameters: "window";  WorkingDir: "{app}"; StatusMsg: "Starting database ..."; Tasks: start and not installsvc

[UninstallRun]
Filename: "{app}\bin\akserver.cmd"; Parameters: "uninstall";  WorkingDir: "{app}"; StatusMsg: "Removing service ..."; Tasks: installsvc

[Code]

function InitializeSetup(): Boolean;
var
  JavaInstalled : Boolean;
begin
  JavaInstalled := RegKeyExists(HKLM,'SOFTWARE\JavaSoft\Java Runtime Environment');
  if JavaInstalled then
    Result := true
  else begin
      MsgBox('Java is required to run Akiban Server', mbError, MB_OK);
      Result := false;
  end;
end;

function ExpandKey(Key: String) : String;
var
  Value: String;
begin
  if Key = 'datadir' then
    Value := '{commonappdata}\Akiban\data'
  else if Key = 'logdir' then
    Value := '{commonappdata}\Akiban\log'
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

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then begin
    EditFile(ExpandConstant('{app}\config\log4j.properties'));
    EditFile(ExpandConstant('{app}\config\server.properties'));
  end;
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

function InstallMode(Param: String): String;
begin
  if IsTaskSelected('installsvc\auto') then
    Result := 'auto'
  else
    Result := 'manual'
end;
