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

package com.foundationdb.server.service.routines;

import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerQueryContext;

@SuppressWarnings("unused") // reflection
public class QueryLoggingRoutines
{
    private QueryLoggingRoutines() {
    }

    private static MonitorService monitorService() {
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        return context.getServer().getServiceManager().getMonitorService();
    }

    public static void setEnabled(boolean enabled) {
        monitorService().setQueryLogEnabled(enabled);
    }

    public static boolean isEnabled() {
        return monitorService().isQueryLogEnabled();
    }

    public static void setFile(String filename) {
        monitorService().setQueryLogFileName(filename);
        // Convenience: If enabled, close and reopen
        if(monitorService().isQueryLogEnabled()) {
            monitorService().setQueryLogEnabled(false);
            monitorService().setQueryLogEnabled(true);
        }
    }

    public static String getFile() {
        return monitorService().getQueryLogFileName();
    }

    public static void setMillis(long millis) {
        monitorService().setQueryLogThresholdMillis(millis);
    }

    public static long getMillis() {
        return monitorService().getQueryLogThresholdMillis();
    }
}
