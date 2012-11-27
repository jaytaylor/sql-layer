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

package com.akiban.server.test.daily.slap;

import static org.junit.Assert.assertTrue;

import com.akiban.server.store.PersistitStoreSchemaManager;
import org.junit.Test;

import com.akiban.ais.model.TableName;
import com.akiban.server.test.daily.DailyBase;

import java.util.Collections;
import java.util.Map;

/**
 * This test simply creates and then drops 2000 tables. Prior to the
 * fix for 772047 this caused an OOME.  Completion of this test constitutes
 * success.
 * @author peter
 *
 */
public class LotsOfTablesDT extends DailyBase {
    private final static int TABLE_COUNT = 2000;

    @Override
    protected Map<String, String> startupConfigProperties() {
        // Set no limit on maximum serialized AIS size
        return Collections.singletonMap(PersistitStoreSchemaManager.MAX_AIS_SIZE_PROPERTY, "0");
    }

    @Test
    public void createLotsOfTablesTest() throws Exception {
        int was = -1;
        for (int count = 0; count < TABLE_COUNT; count++) {
            String tableName = String.format("test%04d", count);
            int tableId = createTable("test", tableName,
                    "I INT NOT NULL, V VARCHAR(255), PRIMARY KEY(I)");
            assertTrue(was != tableId);
            was = tableId;
        }

        for (int count = 0; count < TABLE_COUNT; count++) {
            String tableName = String.format("test%04d", count);
            ddl().dropTable(session(), new TableName("test", tableName));
        }
    }
}
