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

package com.akiban.server.test.it.store;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.server.service.config.Property;
import com.akiban.server.store.PSSMTestShim;
import com.akiban.server.store.PersistitStoreSchemaManager;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.Callable;

import static com.akiban.ais.CAOIBuilderFiller.*;
import static com.akiban.server.store.PersistitStoreSchemaManager.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Test that a failure while upgrading a MetaModel based AIS to
 * a Protobuf based one is handled gracefully with no data loss.
 *
 * Test should be deleted when MetaModel is no longer supported.
 */
public class PersistitStoreSchemaManagerUpgradeFailureIT extends PersistitStoreSchemaManagerITBase {
    private final static String SCHEMA = "test";
    private final static String[] TABLE_LOAD_ORDER = {CUSTOMER_TABLE, ADDRESS_TABLE, ORDER_TABLE, ITEM_TABLE, COMPONENT_TABLE};

    private static class FailingUpgradeHook implements Runnable {
        @Override
        public void run() {
            throw new RuntimeException("Injected failure");
        }
    }

    private static void checkTables(String msg, AkibanInformationSchema ais, TableName... extraTables) {
        // Simple check that tables still exist
        for(String table : TABLE_LOAD_ORDER) {
            assertNotNull(table + " should exist, " + msg, ais.getUserTable(SCHEMA, table));
        }
        for(TableName name : extraTables) {
            assertNotNull(name + " should exist, " + msg, ais.getUserTable(name));
        }
    }

    @Override
    protected boolean defaultDoCleanOnUnload() {
        return false;
    }

    @Test
    public void testFailure() throws Exception {
        final Collection<Property> props = defaultPropertiesToPreserveOnRestart();
        // Clean start
        transactionally(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                PSSMTestShim.clearAISFromDisk(pssm, session());
                return null;
            }
        });

        // Set up non-trivial MetaModel based schema
        pssm.setSerializationType(SerializationType.META_MODEL);
        final AkibanInformationSchema ais = createAndFillBuilder(SCHEMA).ais();
        for(final String table : TABLE_LOAD_ORDER) {
            transactionally(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    pssm.createTableDefinition(session(), ais.getUserTable(SCHEMA, table));
                    return null;
                }
            });
        }

        // Inject our hook and restart
        PSSMTestShim.setUpgradeHook(new FailingUpgradeHook());
        try {
            safeRestart();
            fail("Expected failure during upgrade!");
        } catch(Exception e) {
            // expected
        }

        // Try again with upgrading disabled, hook still active
        props.add(new Property(PersistitStoreSchemaManager.SKIP_AIS_UPGRADE_PROPERTY, "true"));
        safeRestart(props);
        assertEquals("Skipped upgrade serialization", SerializationType.META_MODEL, pssm.getSerializationType());
        checkTables("Skipped upgrade AIS", pssm.getAis(session()),
                    new  TableName("akiban_information_schema", "zindex_statistics"),
                    new  TableName("akiban_information_schema", "zindex_statistics_entry"));

        // Unset hook, restart again (causing upgrade)
        PSSMTestShim.setUpgradeHook(null);
        safeRestart();
        assertEquals("Upgraded serialization", SerializationType.PROTOBUF, pssm.getSerializationType());
        checkTables("Upgraded AIS", pssm.getAis(session()));
    }
}
