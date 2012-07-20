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
LicenseFile=LICENSE.txt

[Tasks]
Name: "installsvc"; Description: "Run as Windows Service"; Flags: unchecked
Name: "installsvc\system"; Description: "Run as System account"; Flags: unchecked
Name: "installsvc\auto"; Description: "Start with Windows"; Flags: unchecked
Name: "start"; Description: "Start now"

[Dirs]
Name: "{code:DataDir}\data"; Permissions: users-modify; Tasks: not installsvc
Name: "{code:DataDir}\data"; Tasks: installsvc
Name: "{code:DataDir}\log"; Permissions: users-modify; Tasks: not installsvc
Name: "{code:DataDir}\log"; Permissions: users-readexec; Tasks: installsvc 

[Files]
Source: "LICENSE.txt"; DestDir: "{app}"
Source: "bin\*"; DestDir: "{app}\bin"
Source: "config\*"; DestDir: "{app}\config"
Source: "lib\*"; DestDir: "{app}\lib"
Source: "procrun\*"; DestDir: "{app}\procrun"; Flags: recursesubdirs

[Icons]
Name: "{group}\Akiban Server"; Filename: "{app}\bin\akserver.cmd"; Parameters: "window";  WorkingDir: "{app}"; Comment: "Run the server as an application"; IconFilename: "{app}\bin\Akiban_Server.ico"
Name: "{group}\Start Service"; Filename: "{app}\bin\akserver.cmd"; Parameters: "start";  WorkingDir: "{app}"; Comment: "Start Windows service"; Tasks: installsvc
Name: "{group}\Stop Service"; Filename: "{app}\bin\akserver.cmd"; Parameters: "stop";  WorkingDir: "{app}"; Comment: "Stop Windows service"; Tasks: installsvc
Name: "{group}\Monitor Service"; Filename: "{app}\bin\akserver.cmd"; Parameters: "monitor";  WorkingDir: "{app}"; Comment: "Monitor Windows service"; Tasks: installsvc
Name: "{group}\Uninstall Akiban Server"; Filename: "{uninstallexe}"

[Run]
;See StartNow() below.
;Filename: "{app}\bin\akserver.cmd"; Parameters: "install -m {code:InstallMode}";  WorkingDir: "{app}"; StatusMsg: "Installing service ..."; Tasks: installsvc
;Filename: "{app}\bin\akserver.cmd"; Parameters: "start";  WorkingDir: "{app}"; StatusMsg: "Starting service ..."; Tasks: start and installsvc
;Filename: "{app}\bin\akserver.cmd"; Parameters: "window";  WorkingDir: "{app}"; StatusMsg: "Starting database ..."; Tasks: start and not installsvc

[UninstallRun]
Filename: "{app}\bin\akserver.cmd"; Parameters: "uninstall";  WorkingDir: "{app}"; StatusMsg: "Removing service ..."; Tasks: installsvc

[Code]
var
  DataDirPage: TInputDirWizardPage;
  ServiceAccountPage: TInputQueryWizardPage;

function InitializeSetup(): Boolean;
var
  JavaInstalled : Boolean;
begin
  JavaInstalled := RegKeyExists(HKLM,'SOFTWARE\JavaSoft\Java Runtime Environment') OR RegKeyExists(HKLM,'SOFTWARE\Wow6432Node\JavaSoft\Java Runtime Environment');
  if JavaInstalled then
    Result := true
  else begin
      MsgBox('Java is required to run Akiban Server', mbError, MB_OK);
      Result := false;
  end;
end;

procedure InitializeWizard;
begin
  DataDirPage := CreateInputDirPage(wpSelectDir,
    'Select Data Directory', 'Where should Akiban data files be installed?',
    'Select the folder in which the database will store its data files, then click Next.',
    False, '');
  DataDirPage.Add('');
  DataDirPage.Values[0] := GetPreviousData('DataDir', ExpandConstant('{commonappdata}\Akiban'));

  ServiceAccountPage := CreateInputQueryPage(wpSelectTasks,
    'Service Account', 'Use what account?',
    'Please specify the account under which the Akiban Server service should run, then click Next.');
  ServiceAccountPage.Add('Username:', False);
  ServiceAccountPage.Add('Password:', True);
  ServiceAccountPage.Add('Repeat password:', True);
  ServiceAccountPage.Values[0] := GetPreviousData('ServiceUsername', '');
end;

procedure RegisterPreviousData(PreviousDataKey: Integer);
begin
  SetPreviousData(PreviousDataKey, 'DataDir', DataDirPage.Values[0]);
  SetPreviousData(PreviousDataKey, 'ServiceUsername', ServiceAccountPage.Values[0]);
end;

function RunServiceAsUser(): Boolean;
begin
  Result := IsTaskSelected('installsvc') and not IsTaskSelected('installsvc\system');
end;

function InstallMode(Param: String): String;
var
  Mode: String;
  Username: String;
begin
  if IsTaskSelected('installsvc\auto') then
    Mode := 'auto'
  else
    Mode := 'manual';
  if RunServiceAsUser() then begin
    Username := ServiceAccountPage.Values[0];
    if (Pos('\', Username) = 0) then Username := '.\' + Username;
    Mode := Mode + ' -su ' + Username + ' ' + ServiceAccountPage.Values[1];
  end;
  Result := Mode;
end;

function ShouldSkipPage(PageID: Integer): Boolean;
begin
  if (PageID = ServiceAccountPage.ID) and not RunServiceAsUser() then
    Result := True
  else                
    Result := False;
end;

function NextButtonClick(CurPageID: Integer): Boolean;
begin
  if CurPageID = ServiceAccountPage.ID then begin
    if (ServiceAccountPage.Values[0] = '') then begin
      MsgBox('You must enter account name.', mbError, MB_OK);
      Result := False;
    end else if (ServiceAccountPage.Values[1] <> ServiceAccountPage.Values[2]) then begin
      MsgBox('Passwords do not match.', mbError, MB_OK);
      Result := False;
    end else
      Result := True;
  end else
    Result := True;
end;

function ExpandKey(Key: String) : String;
var
  Value: String;
begin
  if Key = 'datadir' then
    Value := '{code:DataDir}\data'
  else if Key = 'logdir' then
    Value := '{code:DataDir}\log'
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

procedure StartNow();
var
  Command, Dir, Params: String;
  ResultCode: Integer;
begin
  Command := ExpandConstant('{app}\bin\akserver.cmd');
  Dir := ExpandConstant('{app}');
  if IsTaskSelected('installsvc') then begin
    Params := 'install -m ' + InstallMode('');
    if Exec(Command, Params, Dir, SW_HIDE, ewWaitUntilTerminated, ResultCode) then
      if IsTaskSelected('start') then
        Exec(Command, 'start', Dir, SW_HIDE, ewWaitUntilTerminated, ResultCode);
  end else if IsTaskSelected('start') then
    ExecAsOriginalUser(Command, 'window', Dir, SW_HIDE, ewNoWait, ResultCode);
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then begin
    EditFile(ExpandConstant('{app}\config\log4j.properties'));
    EditFile(ExpandConstant('{app}\config\server.properties'));
    StartNow();
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

function DataDir(Param: String): String;
begin
  Result := DataDirPage.Values[0];
end;
