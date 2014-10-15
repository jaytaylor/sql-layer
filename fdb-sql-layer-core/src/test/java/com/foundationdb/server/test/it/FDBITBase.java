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

import com.foundationdb.server.service.metrics.FDBMetricsService;
import com.foundationdb.server.service.metrics.MetricsService;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager.BindingsConfigurationProvider;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.FDBHolder;
import com.foundationdb.server.store.FDBHolderImpl;
import com.foundationdb.server.store.FDBSchemaManager;
import com.foundationdb.server.store.FDBStore;
import com.foundationdb.server.store.FDBTransactionService;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.statistics.FDBIndexStatisticsService;
import com.foundationdb.server.store.statistics.IndexStatisticsService;

import java.util.Map;

public class FDBITBase extends ITBase
{
    /** For use by classes that cannot extend this class directly */
    public static BindingsConfigurationProvider doBind(BindingsConfigurationProvider provider) {
        return provider.bind(FDBHolder.class, FDBHolderImpl.class)
                       .bind(MetricsService.class, FDBMetricsService.class)
                       .bind(SchemaManager.class, FDBSchemaManager.class)
                       .bind(Store.class, FDBStore.class)
                       .bind(IndexStatisticsService.class, FDBIndexStatisticsService.class)
                       .bind(TransactionService.class, FDBTransactionService.class);
    }

    @Override
    protected BindingsConfigurationProvider serviceBindingsProvider() {
        return doBind(super.serviceBindingsProvider());
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    protected FDBHolder fdbHolder() {
        return serviceManager().getServiceByClass(FDBHolder.class);
    }

    protected FDBMetricsService fdbMetricsService() {
        return (FDBMetricsService)serviceManager().getServiceByClass(MetricsService.class);
    }

    protected FDBSchemaManager fdbSchemaManager() {
        return (FDBSchemaManager)serviceManager().getServiceByClass(SchemaManager.class);
    }

    protected FDBTransactionService fdbTxnService() {
        return (FDBTransactionService)super.txnService();
    }
}
