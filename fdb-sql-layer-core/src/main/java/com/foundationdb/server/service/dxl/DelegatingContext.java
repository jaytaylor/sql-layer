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

package com.foundationdb.server.service.dxl;

import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.QueryContextBase;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.session.Session;

public class DelegatingContext extends QueryContextBase
{
    private final StoreAdapter adapter;
    private final QueryContext delegate;

    public DelegatingContext(StoreAdapter adapter, QueryContext delegate) {
        this.adapter = adapter;
        this.delegate = (delegate == null) ? new SimpleQueryContext(adapter) : delegate;
    }

    @Override
    public StoreAdapter getStore() {
        return adapter;
    }

    @Override
    public StoreAdapter getStore(Table table) {
        return adapter;
    }

    @Override
    public Session getSession() {
        return delegate.getSession();
    }

    @Override
    public ServiceManager getServiceManager() {
        return delegate.getServiceManager();
    }

    @Override
    public String getCurrentUser() {
        return delegate.getCurrentUser();
    }

    @Override
    public String getSessionUser() {
        return delegate.getSessionUser();
    }

    @Override
    public String getCurrentSchema() {
        return delegate.getCurrentSchema();
    }

    @Override
    public String getCurrentSetting(String key) {
        return delegate.getCurrentSetting(key);
    }

    @Override
    public int getSessionId() {
        return delegate.getSessionId();
    }

    @Override
    public void notifyClient(NotificationLevel level, ErrorCode errorCode, String message) {
        delegate.notifyClient(level, errorCode, message);
    }

    @Override
    public long getQueryTimeoutMilli() {
        return delegate.getQueryTimeoutMilli();
    }
}
