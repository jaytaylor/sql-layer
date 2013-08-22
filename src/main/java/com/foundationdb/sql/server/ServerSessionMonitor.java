/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.server;

import com.foundationdb.server.service.monitor.SessionMonitorBase;

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
