
package com.akiban.sql.server;

import com.akiban.server.service.monitor.SessionMonitorBase;

public class ServerSessionMonitor extends SessionMonitorBase {
    private final String serverType;
    private int callerSessionId = -1;
    private String remoteAddress;

    public ServerSessionMonitor(String serverType, int sessionId) {
        super(sessionId);
        this.serverType = serverType;
    }
    
    public void setCallerSessionId(int callerSessionId) {
        this.callerSessionId = callerSessionId;
    }

    public void setRemoteAddress(String remoteAddress) {
        this.remoteAddress = remoteAddress;
    }


    /* SessionMonitor */

    @Override
    public int getCallerSessionId() {
        return callerSessionId;
    }

    @Override
    public String getServerType() {
        return serverType;
    }

    @Override
    public String getRemoteAddress() {
        return remoteAddress;
    }
}
