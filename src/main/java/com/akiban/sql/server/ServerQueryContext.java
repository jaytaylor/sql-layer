/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.server;

import com.akiban.qp.operator.QueryContextBase;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.server.service.session.Session;

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
    public Session getSession() {
        return server.getSession();
    }

    @Override
    public String getCurrentUser() {
        return server.getDefaultSchemaName();
    }

    @Override
    public String getSessionUser() {
        return server.getProperty("user");
    }

    @Override
    public void notifyClient(NOTIFICATION_LEVEL level, String message) {
        try {
            server.notifyClient(level, message);
        }
        catch (IOException ex) {
        }
    }

}
