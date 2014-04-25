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

package com.foundationdb.server.store.statistics;

import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.PersistitStore;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.google.inject.Inject;

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
}
