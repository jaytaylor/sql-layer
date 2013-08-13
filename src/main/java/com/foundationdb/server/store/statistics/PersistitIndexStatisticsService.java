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

package com.foundationdb.server.store.statistics;

import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.qp.persistitadapter.PersistitAdapter;
import com.foundationdb.server.AccumulatorAdapter;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.PersistitStore;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.google.inject.Inject;
import com.persistit.Exchange;
import com.persistit.exception.PersistitInterruptedException;

public class PersistitIndexStatisticsService extends AbstractIndexStatisticsService {
    private final PersistitStore store;

    @Inject
    public PersistitIndexStatisticsService(Store store,
                                           TransactionService txnService,
                                           SchemaManager schemaManager,
                                           SessionService sessionService,
                                           ConfigurationService configurationService,
                                           ListenerService listenerService) {
        super(store, txnService, schemaManager, sessionService, configurationService, listenerService);
        if(store instanceof PersistitStore) {
            this.store = (PersistitStore)store;
        } else {
            throw new IllegalStateException("Must be used with PersistitStore");
        }
    }

    @Override
    protected AbstractStoreIndexStatistics createStoreIndexStatistics() {
        return new PersistitStoreIndexStatistics(store, this);
    }

    @Override
    protected long countGIEntries(Session session, GroupIndex index) {
        Exchange ex = store.getExchange(session, index);
        try {
            return AccumulatorAdapter.getSnapshot(AccumulatorAdapter.AccumInfo.ROW_COUNT, ex.getTree());
        } catch(PersistitInterruptedException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            store.releaseExchange(session, ex);
        }
    }

    @Override
    protected long countGIEntriesApproximate(Session session, GroupIndex index) {
        Exchange ex = store.getExchange(session, index);
        try {
            return AccumulatorAdapter.getLiveValue(AccumulatorAdapter.AccumInfo.ROW_COUNT, ex.getTree());
        }
        finally {
            store.releaseExchange(session, ex);
        }
    }
}
