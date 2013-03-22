
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
