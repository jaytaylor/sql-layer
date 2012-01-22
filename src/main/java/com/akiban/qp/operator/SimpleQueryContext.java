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

package com.akiban.qp.operator;

import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.server.service.session.Session;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link QueryContext} for use without a full server for internal plans / testing. */
public class SimpleQueryContext extends QueryContextBase
{
    private static final Logger logger = LoggerFactory.getLogger(SimpleQueryContext.class);

    private StoreAdapter adapter;

    public SimpleQueryContext(StoreAdapter adapter) {
        this.adapter = adapter;
    }

    @Override
    public StoreAdapter getStore() {
        return adapter;
    }

    @Override
    public Session getSession() {
        if (adapter instanceof PersistitAdapter)
            return ((PersistitAdapter)adapter).session();
        else
            throw new UnsupportedOperationException();
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
    public void notifyClient(NOTIFICATION_LEVEL level, String message) {
        switch (level) {
        case WARNING:
            logger.warn(message);
            break;
        case INFO:
            logger.info(message);
            break;
        case DEBUG:
            logger.debug(message);
            break;
        }
    }

}
