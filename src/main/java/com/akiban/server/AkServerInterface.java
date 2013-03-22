
package com.akiban.server;

public interface AkServerInterface
{
    String getServerName();
    String getServerVersion();
    String getServerShortVersion();
    int getServerMajorVersion();
    int getServerMinorVersion();
    int getServerPatchVersion();
}
