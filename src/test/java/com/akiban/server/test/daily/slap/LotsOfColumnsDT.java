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

import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import com.akiban.ais.model.TableName;
import com.akiban.server.store.PersistitStoreSchemaManager;
import com.akiban.server.test.daily.DailyBase;

public class LotsOfColumnsDT extends DailyBase {
    private final static int COLUMN_COUNT = 10000;
    @Override
    protected Map<String, String> startupConfigProperties() {
        // Set no limit on maximum serialized AIS size
        return Collections.singletonMap(PersistitStoreSchemaManager.MAX_AIS_SIZE_PROPERTY, "0");
    }

    @Test
    public void testIntColumns() {
        runTest("INT", "test_1");
    }
    
    @Test
    public void testCharColumns() {
        runTest("CHAR(1)", "table_2");
    }
    
    @Test
    public void testDoubleColumns() {
        runTest("DOUBLE", "table_3");
    }
    
    @Test
    public void testVarcharSmallColumns() {
        runTest("VARCHAR(5)", "table_4");
    }
    
    @Test
    public void testVarcharLargeColumns() {
        runTest("VARCHAR(300)", "table_5");
    }
    
    @Test
    public void testDecimalSmall() {
        runTest ("DECIMAL(3,2)", "table_6");
    }

    @Test
    public void testDecimalLarge() {
        runTest("DECIMAL(31, 21)", "table_7");
    }
    
    private void runTest (String type, String tableName) {
        StringBuilder query = new StringBuilder(COLUMN_COUNT * 32);
        
        generateColumnList (type, query);
        query.append("ID INT NOT NULL PRIMARY KEY");
        createTable("test", tableName, query.toString());

        ddl().dropTable(session(), new TableName("test", tableName));
    }

    private void generateColumnList (String type, StringBuilder query) {
        for (int i = 0; i < COLUMN_COUNT; i++) {
            query.append(String.format("COLUMN_%05d %s NOT NULL,", i, type));
        }
        
    }
}
