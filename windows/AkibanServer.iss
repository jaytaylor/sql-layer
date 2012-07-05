#ifndef VERSION
#define VERSION "1.0-UNKNOWN"
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

[Dirs]
Name: "{commonappdata}\Akiban\data"
Name: "{commonappdata}\Akiban\log"

[Files]
Source: "LICENSE.txt"; DestDir: "{app}";
Source: "bin\*"; DestDir: "{app}\bin";
Source: "config\*"; DestDir: "{app}\config";
Source: "lib\*"; DestDir: "{app}\lib";
Source: "procrun\*"; DestDir: "{app}\procrun";

[Icons]
Name: "{group}\Akiban Server"; Filename: "{app}\bin\akserver.cmd"; Parameters: "window";  WorkingDir: "{app}"; Comment: "Run the server as an application"; IconFilename: "{app}\bin\Akiban_Server.ico"

[Code]

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
