package com.akiban.cserver.itests.bugs.bug704443;

import com.akiban.ais.io.CSVTarget;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.itests.ApiTestBase;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static junit.framework.Assert.*;

public final class DecimalsWithoutParamsIT extends ApiTestBase {
    @Test
    public void decimalHasNoParams() throws InvalidOperationException {
        TableId t1Id = createTable("schema", "t1",
                "c1 DECIMAL NOT NULL, c2 DECIMAL NOT NULL, c3 DECIMAL NOT NULL, PRIMARY KEY(c1,c2,c3)"
        );

        Set<TableName> t1TableNameSet = new HashSet<TableName>();
        t1TableNameSet.add(new TableName("schema", "t1"));

        assertEquals("user tables", t1TableNameSet, getUserTables().keySet());
        assertEquals("group tables size", 1, getGroupTables().size());

        writeRows(
                createNewRow(t1Id, "10", "10", "10")
        );
        expectFullRows( t1Id,
                createNewRow(t1Id, new BigDecimal("10"), new BigDecimal("10"), new BigDecimal("10"))
        );

        for(Column column : getUserTable(t1Id).getColumns()) {
            assertEquals("param[0] for column " + column, Long.valueOf(10), column.getTypeParameter1());
            assertEquals("param[1] for column " + column, Long.valueOf(0), column.getTypeParameter2());
        }
        assertEquals("columns.size()", 3, getUserTable(t1Id).getColumns().size());
        
        columnSerializationTest(t1Id);
    }

    @Test
    public void decimalHasOneParams() throws InvalidOperationException {
        TableId t1Id = createTable("schema", "t1",
                "c1 DECIMAL(17) NOT NULL,PRIMARY KEY(c1)"
        );

        Set<TableName> t1TableNameSet = new HashSet<TableName>();
        t1TableNameSet.add(new TableName("schema", "t1"));

        assertEquals("user tables", t1TableNameSet, getUserTables().keySet());
        assertEquals("group tables size", 1, getGroupTables().size());

        writeRows(
                createNewRow(t1Id, "27")
        );
        expectFullRows( t1Id,
                createNewRow(t1Id, new BigDecimal("27"))
        );
        for(Column column : getUserTable(t1Id).getColumns()) {
            assertEquals("param[0] for column " + column, Long.valueOf(17), column.getTypeParameter1());
            assertEquals("param[1] for column " + column, Long.valueOf(0), column.getTypeParameter2());
        }
        assertEquals("columns.size()", 1, getUserTable(t1Id).getColumns().size());

        columnSerializationTest(t1Id);
    }

    /**
     * Bug 706315. For each column in the given table, compares the column's values to the values of its map.
     * @param tableId the table whose columns we should compare
     */
    private void columnSerializationTest(TableId tableId) throws InvalidOperationException {
        TableName tableName = ddl().getTableName(tableId);
        UserTable uTable = ddl().getAIS(session).getUserTable(tableName);
        assertNotNull("no table " + tableName, uTable);

        for (Column column : uTable.getColumns()) {
            Set<String> keysChecked = new TreeSet<String>();
            Map<String,Object> map = Collections.unmodifiableMap(new TreeMap<String,Object>(column.map()));

            checkKey(keysChecked, map, "charset", column.getCharsetAndCollation().charset());
            checkKey(keysChecked, map, "collation", column.getCharsetAndCollation().collation());
            checkKey(keysChecked, map, "columnName", column.getName());
            checkKey(keysChecked, map, "groupColumnName", column.getGroupColumn().getName());
            checkKey(keysChecked, map, "groupSchemaName", column.getGroupColumn().getTable().getName().getSchemaName());
            checkKey(keysChecked, map, "groupTableName", column.getGroupColumn().getTable().getName().getTableName());
            checkKey(keysChecked, map, "initialAutoIncrementValue", column.getInitialAutoIncrementValue());
            checkKey(keysChecked, map, "maxStorageSize", column.getMaxStorageSize());
            checkKey(keysChecked, map, "nullable", column.getNullable());
            checkKey(keysChecked, map, "position", column.getPosition());
            checkKey(keysChecked, map, "prefixSize", column.getPrefixSize());
            checkKey(keysChecked, map, "schemaName", column.getTable().getName().getSchemaName());
            checkKey(keysChecked, map, "tableName", column.getTable().getName().getTableName());
            checkKey(keysChecked, map, "typename", column.getType().name());
            checkKey(keysChecked, map, "typeParam1", column.getTypeParameter1());
            checkKey(keysChecked, map, "typeParam2", column.getTypeParameter2());
            
            assertEquals("keys checked", map.keySet(), keysChecked);
        }
    }

    private static void checkKey(Set<String> checkedKeys, Map<String,Object> map, String key, Object value) {
        boolean added = checkedKeys.add(key);
        assertTrue(key + " already checked", added);
        assertTrue(key + " not in map: " + map, map.containsKey(key));
        assertEquals(key, value, map.get(key));
    }
}
