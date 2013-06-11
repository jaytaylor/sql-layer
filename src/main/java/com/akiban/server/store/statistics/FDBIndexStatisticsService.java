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

package com.akiban.server.store.statistics;

import com.akiban.ais.model.GroupIndex;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.FDBStore;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FDBIndexStatisticsService extends AbstractIndexStatisticsService {
    private static final Logger LOG = LoggerFactory.getLogger(FDBIndexStatisticsService.class.getName());

    private final FDBStore store;

    @Inject
    public FDBIndexStatisticsService(Store store,
                                     TransactionService txnService,
                                     SchemaManager schemaManager,
                                     SessionService sessionService,
                                     ConfigurationService configurationService) {
        super(store, txnService, schemaManager, sessionService, configurationService);
        if(store instanceof FDBStore) {
            this.store = (FDBStore)store;
        } else {
            throw new IllegalArgumentException("Must be started with FDBStore");
        }
    }

    @Override
    protected AbstractStoreIndexStatistics createStoreIndexStatistics() {
        return new FDBStoreIndexStatistics(store, this);
    }

    @Override
    protected long countGIEntries(Session session, GroupIndex index) {
        LOG.warn("Faking countGIEntries");
        // TODO: Generalize GI count tracking
        long rootCount = index.rootMostTable().rowDef().getTableStatus().getRowCount(session);
        long leafCount = index.leafMostTable().rowDef().getTableStatus().getRowCount(session);
        return rootCount * leafCount;

    }

    @Override
    protected long countGIEntriesApproximate(Session session, GroupIndex index) {
        LOG.warn("Faking countGIEntries");
        // TODO: Generalize GI count tracking
        long rootCount = index.rootMostTable().rowDef().getTableStatus().getApproximateRowCount();
        long leafCount = index.leafMostTable().rowDef().getTableStatus().getApproximateRowCount();
        return rootCount * leafCount;
    }
}
