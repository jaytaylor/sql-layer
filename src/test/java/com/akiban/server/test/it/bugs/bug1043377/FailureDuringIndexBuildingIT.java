/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.it.bugs.bug1043377;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.persistitadapter.OperatorStore;
import com.akiban.server.service.lock.LockService;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.service.session.Session;
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
        return super.serviceBindingsProvider().bind(Store.class, ThrowsAfterBuildIndexesStore.class);
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
        public ThrowsAfterBuildIndexesStore(TreeService treeService, SchemaManager schemaManager, LockService lockService) {
            super(treeService, null, schemaManager, lockService);
        }

        @Override
        public void buildIndexes(Session session, Collection<? extends Index> indexes, boolean deferIndexes) {
            super.buildIndexes(session, indexes, deferIndexes);
            throw EXPECTED_EXCEPTION;
        }
    }
}
