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

import com.akiban.ais.AISComparator;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.store.PSSMTestShim;
import com.akiban.server.store.PersistitStoreSchemaManager;
import org.junit.Test;

import java.util.concurrent.Callable;

import static com.akiban.ais.CAOIBuilderFiller.*;
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
    private static class FailingUpgradeHook implements Runnable {
        @Override
        public void run() {
            throw new RuntimeException("Injected failure");
        }
    }

    @Test
    public void testFailure() throws Exception {
        // Clean start
        transactionally(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                PSSMTestShim.clearAISFromDisk(pssm, session());
                return null;
            }
        });


        // Set up mildly non-trivial, MetaModel based schema
        pssm.setSerializationType(PersistitStoreSchemaManager.SerializationType.META_MODEL);

        final String SCHEMA = "test";
        final String[] LOAD_ORDER = {CUSTOMER_TABLE, ADDRESS_TABLE, ORDER_TABLE, ITEM_TABLE, COMPONENT_TABLE};
        final AkibanInformationSchema ais = createAndFillBuilder(SCHEMA).ais();
        for(final String table : LOAD_ORDER) {
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

        // Unset hook, restart again (causing upgrade)
        PSSMTestShim.setUpgradeHook(null);
        safeRestart();

        // Check state of upgraded schema
        assertEquals("Upgraded serialization", PersistitStoreSchemaManager.SerializationType.PROTOBUF, pssm.getSerializationType());
        AkibanInformationSchema upgradedAIS = pssm.getAis(session());
        // Simple check that tables still exist
        for(String table : LOAD_ORDER) {
            assertNotNull(table + " exists after upgrade", upgradedAIS.getUserTable(SCHEMA, table));
        }
    }
}
