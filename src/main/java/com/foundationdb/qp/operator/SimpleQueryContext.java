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

package com.foundationdb.qp.operator;

import com.foundationdb.ais.model.Table;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.session.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link QueryContext} for use without a full server for internal plans / testing. */
public class SimpleQueryContext extends QueryContextBase
{
    private static final Logger logger = LoggerFactory.getLogger(SimpleQueryContext.class);

    private final StoreAdapter adapter;
    private final ServiceManager serviceManager;
    private  String sqlStatement;

    public SimpleQueryContext() {
        this(null);
    }

    public SimpleQueryContext(StoreAdapter adapter, ServiceManager serviceManager, String sqlStatement){
        this(adapter, serviceManager);
        this.sqlStatement = sqlStatement;
    }

    public SimpleQueryContext(StoreAdapter adapter) {
        this(adapter, null);
    }

    public SimpleQueryContext(StoreAdapter adapter, ServiceManager serviceManager) {
        this.adapter = adapter;
        this.serviceManager = serviceManager;
    }

    @Override
    public StoreAdapter getStore() {
        requireAdapter();
        return adapter;
    }

    @Override
    public StoreAdapter getStore(Table table) {
        requireAdapter();
        return adapter;
    }
    
    @Override
    public Session getSession() {
    	if (adapter != null)
            return adapter.getSession();
    	else
            return null;
    }

    @Override
    public ServiceManager getServiceManager() {
        requireServiceManager();
        return serviceManager;
    }

    @Override
    public String getCurrentUser() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSessionUser() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCurrentSchema() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCurrentSetting(String key) {
        return null;
    }

    @Override
    public int getSessionId() {
        requireAdapter();
        return (int)adapter.getSession().sessionId();
    }

    @Override
    public void notifyClient(NotificationLevel level, ErrorCode errorCode, String message) {
        switch (level) {
        case WARNING:
            logger.warn("{} {}", errorCode, message);
            break;
        case INFO:
            logger.info("{} {}", errorCode, message);
            break;
        case DEBUG:
            logger.debug("{} {}", errorCode, message);
            break;
        }
    }

    @Override
    public void checkQueryCancelation() {
        if (adapter.getSession() != null) {
           super.checkQueryCancelation();
        }
    }

    private void requireAdapter() {
        if(adapter == null) {
            throw new UnsupportedOperationException("Not constructed with StoreAdapter");
        }
    }

    private void requireServiceManager() {
        if(serviceManager == null) {
            throw new UnsupportedOperationException("Not constructed with ServiceManager");
        }
    }
}
