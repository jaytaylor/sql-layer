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

package com.akiban.server.test.it.bugs.bug1043377;

import com.akiban.server.service.text.FullTextIndexServiceImpl;
import com.akiban.server.service.text.FullTextIndexService;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.persistitadapter.OperatorStore;
import com.akiban.server.service.lock.LockService;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.server.test.it.ITBase;
import com.google.inject.Inject;
import org.junit.Test;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public final class FailureDuringIndexBuildingIT extends ITBase {
    private static final AssertionError EXPECTED_EXCEPTION = new AssertionError();

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider().bind(Store.class, ThrowsAfterBuildIndexesStore.class)
                                              .bind(FullTextIndexService.class, FullTextIndexServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Test
    public void injectedFailure() throws Throwable {
        final String SCHEMA = "test";
        final String TABLE = "t1";
        final String INDEX = "lat_lon";
        int tid = createTable(SCHEMA, TABLE, "userID int not null primary key, lat decimal(11,7), lon decimal(11,7)");
        writeRows(
                createNewRow(tid, 1L, "20.5", "11.0"),
                createNewRow(tid, 2L, "90.0", "90.0"),
                createNewRow(tid, 3L, "60.2", "5.34")
        );

        try {
            createIndex(SCHEMA, TABLE, INDEX, "lat", "lon");
            fail("Expected exception");
        } catch(Throwable t) {
            if(t != EXPECTED_EXCEPTION) {
                throw t;
            }
        }

        UserTable table = getUserTable(SCHEMA, TABLE);
        assertNull("Index should not be present", table.getIndex(INDEX));
    }

    public static class ThrowsAfterBuildIndexesStore extends OperatorStore {
        @Inject
        public ThrowsAfterBuildIndexesStore(TreeService treeService, SchemaManager schemaManager,
                                            LockService lockService, TransactionService transactionService,
                                            FullTextIndexService ftService) {
            super(treeService, null, schemaManager, lockService, transactionService, ftService);
        }

        @Override
        public void buildIndexes(Session session, Collection<? extends Index> indexes, boolean deferIndexes) {
            super.buildIndexes(session, indexes, deferIndexes);
            throw EXPECTED_EXCEPTION;
        }
    }
}
