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

package com.foundationdb.server.test.it;

import com.foundationdb.server.service.servicemanager.GuicedServiceManager.BindingsConfigurationProvider;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.service.tree.TreeService;
import com.foundationdb.server.service.tree.TreeServiceImpl;
import com.foundationdb.server.store.PersistitStore;
import com.foundationdb.server.store.PersistitStoreSchemaManager;
import com.foundationdb.server.store.PersistitTransactionService;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.server.store.statistics.PersistitIndexStatisticsService;

import java.util.Map;

/** Base class for ITs testing Persistit specific behavior or implementations. */
public abstract class PersistitITBase extends ITBase
{
    /** For use by classes that cannot extend this class directly */
    public static BindingsConfigurationProvider doBind(BindingsConfigurationProvider provider) {
        return provider.bind(TreeService.class, TreeServiceImpl.class)
                       .bind(SchemaManager.class, PersistitStoreSchemaManager.class)
                       .bind(Store.class, PersistitStore.class)
                       .bind(IndexStatisticsService.class, PersistitIndexStatisticsService.class)
                       .bind(TransactionService.class, PersistitTransactionService.class);
    }

    @Override
    protected BindingsConfigurationProvider serviceBindingsProvider() {
        return doBind(super.serviceBindingsProvider());
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Override
    public final void safeRestartTestServices(Map<String, String> propertiesToPreserve) throws Exception {
        /*
         * Need this because deleting Trees currently is not transactional.  Therefore after
         * restart we recover the previous trees and forget about the deleteTree operations.
         * TODO: remove when transaction Tree management is done.
         */
        treeService().getDb().checkpoint();
        super.safeRestartTestServices(propertiesToPreserve);
    }

    protected final TreeService treeService() {
        return serviceManager().getServiceByClass(TreeService.class);
    }
}
