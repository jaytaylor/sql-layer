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

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.QueryContextBase;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.operator.StoreAdapterHolder;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.types.common.types.TypesTranslator;

import java.io.IOException;

public class ServerQueryContext<T extends ServerSession> extends QueryContextBase
{
    private final T server;
    private StoreAdapterHolder storeHolder;

    public ServerQueryContext(T server) {
        this.server = server;
    }

    public T getServer() {
        return server;
    }

    public void initStore(Schema schema) {
        storeHolder = server.getStoreHolder(schema);
    }

    @Override
    public StoreAdapter getStore() {
        assert (storeHolder != null) : "init() not called";
        return storeHolder.getAdapter();
    }

    @Override
    public StoreAdapter getStore(Table table) {
        assert (storeHolder != null) : "init() not called";
        return storeHolder.getAdapter(table);
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
    public String getCurrentSetting(String key) {
        return server.getSessionSetting(key);
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
    public ServerTransaction.PeriodicallyCommit getTransactionPeriodicallyCommit() {
        return server.getTransactionPeriodicallyCommit();
    }

    public TypesTranslator getTypesTranslator() {
        return server.typesTranslator();
    }
}
