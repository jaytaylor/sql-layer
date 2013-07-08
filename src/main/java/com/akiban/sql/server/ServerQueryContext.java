/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.sql.server;

import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.QueryBindings;
import com.akiban.qp.operator.QueryContextBase;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.QueryCanceledException;
import com.akiban.server.error.QueryTimedOutException;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.dxl.DXLReadWriteLockHook;
import com.akiban.server.service.session.Session;
import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;

import java.io.IOException;

public class ServerQueryContext<T extends ServerSession> extends QueryContextBase
{
    private T server;

    public ServerQueryContext(T server) {
        this.server = server;
    }

    public T getServer() {
        return server;
    }

    @Override
    public StoreAdapter getStore() {
        return server.getStore();
    }

    @Override
    public StoreAdapter getStore(UserTable table) {
        return server.getStore(table);
    }

    @Override
    public Session getSession() {
        return server.getSession();
    }

    @Override
    public ServiceManager getServiceManager() {
        return server.getServiceManager();
    }

    @Override
    public String getCurrentUser() {
        return getSessionUser();
    }

    @Override
    public String getSessionUser() {
        return server.getProperty("user");
    }

    @Override
    public String getCurrentSchema() {
        return server.getDefaultSchemaName();
    }

    @Override
    public int getSessionId() {
        return server.getSessionMonitor().getSessionId();
    }

    @Override
    public void notifyClient(NotificationLevel level, ErrorCode errorCode, String message) {
        try {
            server.notifyClient(level, errorCode, message);
        }
        catch (IOException ex) {
        }
    }

    @Override
    public long getQueryTimeoutMilli() {
        return server.getQueryTimeoutMilli();
    }

    @Override
    public long sequenceNextValue(TableName sequenceName) {
        return server.getStore().sequenceNextValue(sequenceName);
    }

    @Override
    public long sequenceCurrentValue(TableName sequenceName) {
        return server.getStore().sequenceCurrentValue(sequenceName);
    }

    public void lock(DXLFunction operationType) {
        long timeout = 0;       // No timeout.
        long queryTimeoutMilli = getQueryTimeoutMilli();
        if (queryTimeoutMilli >= 0) {
            long runningTimeMsec = System.currentTimeMillis() - getStartTime();
            timeout = queryTimeoutMilli - runningTimeMsec;
            if (timeout <= 0) {
                // Already past time.
                throw new QueryTimedOutException(runningTimeMsec);
            }
        }
        try {
            boolean locked = DXLReadWriteLockHook.only().lock(getSession(), operationType, timeout);
            if (!locked) {
                throw new QueryTimedOutException(System.currentTimeMillis() - getStartTime());
            }
        }
        catch (InterruptedException ex) {
            throw new QueryCanceledException(getSession());
        }
    }

    public void unlock(DXLFunction operationType) {
        DXLReadWriteLockHook.only().unlock(getSession(), operationType);
    }

}
