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
