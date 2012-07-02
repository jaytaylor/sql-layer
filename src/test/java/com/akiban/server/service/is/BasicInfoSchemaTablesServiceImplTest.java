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

package com.akiban.server.service.is;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.memoryadapter.MemoryAdapter;
import com.akiban.qp.memoryadapter.MemoryTableFactory;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.store.AisHolder;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.akiban.qp.memoryadapter.MemoryGroupCursor.GroupScan;
import static com.akiban.server.types.AkType.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class BasicInfoSchemaTablesServiceImplTest {
    private static final String I_S = TableName.INFORMATION_SCHEMA;

    private AisHolder holder;
    private BasicInfoSchemaTablesServiceImpl bist;
    private MemoryAdapter adapter;

    @Before
    public void setUp() {
        holder = new AisHolder() {
            final AkibanInformationSchema ais = BasicInfoSchemaTablesServiceImpl.createTablesToRegister();

            @Override
            public AkibanInformationSchema getAis() {
                return ais;
            }

            @Override
            public void setAis(AkibanInformationSchema ais) {
                throw new UnsupportedOperationException();
            }
        };
        createTables();
        bist = new BasicInfoSchemaTablesServiceImpl(holder, null);
        bist.attachFactories(holder.getAis(), false);
        adapter = new MemoryAdapter(new Schema(holder.getAis()), null, null);
    }

    @After
    public void tearDown() {
        holder = null;
        bist = null;
        adapter = null;
    }

    private void createTables() {
        AISBuilder builder = new AISBuilder(holder.getAis());

        String schema = "test";
        String table = "foo";
        builder.userTable(schema, table);
        builder.column(schema, table, "c1", 0, "INT", null, null, false, false, null, null);
        builder.column(schema, table, "c2", 1, "DOUBLE", null, null, true, false, null, null);
        builder.createGroup(table, schema, "_akiban_"+table);
        builder.addTableToGroup(table, schema, table);
        // no defined pk or indexes

        schema = "test";
        table = "bar";
        builder.userTable(schema, table);
        builder.column(schema, table, "col", 0, "BIGINT", null, null, false, false, null, null);
        builder.column(schema, table, "name", 1, "INT", null, null, false, false, null, null);
        builder.index(schema, table, Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(schema, table, Index.PRIMARY_KEY_CONSTRAINT, "col", 0, true, null);
        builder.createGroup(table, schema, "_akiban_"+table);

        schema = "test";
        String childTable = table + "2";
        String indexName = "foo_name";
        builder.userTable(schema, childTable);
        builder.column(schema, childTable, "foo", 0, "INT", null, null, true, false, null, null);
        builder.column(schema, childTable, "pid", 1, "INT", null, null, true, false, null, null);

        String joinName = childTable + "/" + table;
        builder.joinTables(joinName, schema, table, schema, childTable);
        builder.joinColumns(joinName, schema, table, "col", schema, childTable, "pid");
        builder.addJoinToGroup(table, joinName, 0);

        builder.groupIndex(table, indexName, false, Index.JoinType.RIGHT);
        builder.groupIndexColumn(table, indexName, schema, childTable, "foo", 0);
        builder.groupIndexColumn(table, indexName, schema, table, "name", 1);

        schema = "zap";
        table = "pow";
        indexName = "name_value";
        builder.userTable(schema, table);
        builder.column(schema, table, "name", 0, "VARCHAR", 32L, null, true, false, null, null);
        builder.column(schema, table, "value", 1, "DECIMAL", 10L, 2L, true, false, null, null);
        builder.index(schema, table, indexName, true, Index.UNIQUE_KEY_CONSTRAINT);
        builder.indexColumn(schema, table, indexName, "name", 0, true, null);
        builder.indexColumn(schema, table, indexName, "value", 1, true, null);
        builder.createGroup(table, schema, "_akiban_"+table);
        builder.addTableToGroup(table, schema, table);
        // no defined pk

        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();

        Map<Table, Integer> ordinalMap = new HashMap<Table, Integer>();
        for(UserTable userTable : holder.getAis().getUserTables().values()) {
            ordinalMap.put(userTable, 0);
            userTable.setTreeName(userTable.getName().getTableName() + "_tree");
            for(Index index : userTable.getIndexesIncludingInternal()) {
                index.computeFieldAssociations(ordinalMap);
                index.setTreeName(index.getIndexName().getName() + "_tree");
            }
        }
        for(Group group : holder.getAis().getGroups().values()) {
            for(Index index : group.getIndexes()) {
                index.computeFieldAssociations(ordinalMap);
                index.setTreeName(index.getIndexName().getName() + "_tree");
            }
        }
    }

    private MemoryTableFactory getFactory(TableName name) {
        UserTable table = holder.getAis().getUserTable(name);
        assertNotNull("No such table: " + name, table);
        MemoryTableFactory factory = table.getMemoryTableFactory();
        assertNotNull("No factory for table " + name, factory);
        return factory;
    }

    /**
     * Performing a full scan and compare against expected rows. This skips all I_S
     * rows and returns that as a value, as double checking all those in this test
     * excessive.
     *
     * @param expectedRows Expected, non-I_S rows.
     * @param scan Scan providing actual rows.
     *
     * @return The number of I_S rows seen.
     */
    private static int scanAndCompare(Object[][] expectedRows, GroupScan scan) {
        int skippedRows = 0;

        Row row;
        int rowIndex = 0;
        while((row = scan.next()) != null) {
            if(rowIndex == expectedRows.length) {
                fail("More actual rows than expected");
            }
            assertEquals("Expected column count, row " + rowIndex,
                         expectedRows[rowIndex].length, row.rowType().nFields());

            for(int colIndex = 0; colIndex < expectedRows[rowIndex].length; ++colIndex) {
                final String msg = "row " + rowIndex + ", col " + colIndex;
                final Object expected = expectedRows[rowIndex][colIndex];
                final ValueSource actual = row.eval(colIndex);

                if(expected == null) {
                    assertEquals(msg + " isNull", true, actual.isNull());
                } else if(expected instanceof AkType) {
                    assertEquals(msg + " (type only)", expected, actual.getConversionType());
                } else if(expected instanceof String) {
                    if(colIndex == 0 && actual.getString().equals(I_S)) {
                        --rowIndex;
                        ++skippedRows;
                        break;
                    }
                    assertEquals(msg, expected, actual.getString());
                } else if(expected instanceof Integer) {
                    assertEquals(msg, expected, actual.getInt());
                } else if(expected instanceof Long) {
                    assertEquals(msg, expected, actual.getLong());
                } else if(expected instanceof Boolean) {
                    assertEquals(msg, (Boolean)expected ? "YES" : "NO", actual.getString());
                } else {
                    fail("Unsupported type: " + expected.getClass());
                }
            }
            ++rowIndex;
        }
        if(rowIndex < expectedRows.length) {
            fail("More expected rows than actual");
        }
        scan.close();

        return skippedRows;
    }


    @Test
    public void schemataScan() {
        final Object[][] expected = {
                { "test", null, null, null, LONG },
                { "zap", null, null, null, LONG }
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.SCHEMATA).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S schemas", 1, skipped);
    }

    @Test
    public void tablesScan() {
        final Object[][] expected = {
                { "test", "bar", "TABLE", LONG, "bar_tree", I_S, VARCHAR, I_S, VARCHAR, LONG },
                { "test", "bar2", "TABLE", LONG, "bar2_tree", I_S, VARCHAR, I_S, VARCHAR, LONG },
                { "test", "foo", "TABLE", LONG, "foo_tree", I_S, VARCHAR, I_S, VARCHAR, LONG },
                { "zap", "pow", "TABLE", LONG, "pow_tree", I_S, VARCHAR, I_S, VARCHAR, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.TABLES).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S tables", 9, skipped);
    }

    @Test
    public void columnsScan() {
        final Object[][] expected = {
                { "test", "bar", "col", 0L, "bigint", false, 8L, null, null, 0L, null, null, null, null, null, LONG },
                { "test", "bar", "name", 1L, "int", false, 4L, null, null, 0L, null, null, null, null, null, LONG },
                { "test", "bar2", "foo", 0L, "int", true, 4L, null, null, 0L, null, null, null, null, null, LONG },
                { "test", "bar2", "pid", 1L, "int", true, 4L, null, null, 0L, null, null, null, null, null, LONG },
                { "test", "foo", "c1", 0L, "int", false, 4L, null, null, 0L, null, null, null, null, null, LONG },
                { "test", "foo", "c2", 1L, "double", true, 8L, null, null, 0L, null, null, null, null, null, LONG },
                { "zap", "pow", "name", 0L, "varchar", true, 32L, null, null, 1L, null, "information_schema", VARCHAR, "information_schema", VARCHAR, LONG },
                { "zap", "pow", "value", 1L, "decimal", true, 5L, 10L, 2L, 0L, null, null, null, null, null, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.COLUMNS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S columns", 70, skipped);
    }

    @Test
    public void tableConstraintsScan() {
        final Object[][] expected = {
                { "test", "bar", "PRIMARY", "PRIMARY KEY", LONG },
                { "test", "bar2", "bar2/bar", "GROUPING", LONG },
                { "zap", "pow", "name_value", "UNIQUE", LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.TABLE_CONSTRAINTS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S constraints", 0, skipped);
    }

    @Test
    public void referentialConstraintsScan() {
        final Object[][] expected = {
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.REFERENTIAL_CONSTRAINTS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S referential_constraints", 0, skipped);
    }

    @Test
    public void groupingConstraintsScan() {
        final Object[][] expected = {
                { "test", "bar2", "bar2/bar", "test", "bar", "PRIMARY", LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.GROUPING_CONSTRAINTS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S grouping_constraints", 0, skipped);
    }

    @Test
    public void keyColumnUsageScan() {
        final Object[][] expected = {
                { "test", "bar", "PRIMARY", "col", 0L, null, LONG },
                { "test", "bar2", "bar2/bar", "pid", 0L, 0L, LONG },
                { "zap", "pow", "name_value", "name", 0L, null, LONG },
                { "zap", "pow", "name_value", "value", 1L, null, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.KEY_COLUMN_USAGE).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S key_column_usage", 0, skipped);
    }

    @Test
    public void indexesScan() {
        final Object[][] expected = {
                { "test", "bar", "PRIMARY", "PRIMARY", LONG, "PRIMARY_tree", "PRIMARY", true, null, LONG },
                { "test", "bar2", "foo_name", null, LONG, "foo_name_tree", "INDEX", false, "RIGHT", LONG },
                { "zap", "pow", "name_value", "name_value", LONG, "name_value_tree", "UNIQUE", true, null, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.INDEXES).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S indexes", 0, skipped);
    }

    @Test
    public void indexColumnsScan() {
        final Object[][] expected = {
                { "test", "PRIMARY", "bar", "test", "bar", "col", 0L, true, null, LONG },
                { "test", "foo_name", "bar2", "test", "bar2", "foo", 0L, true, null, LONG },
                { "test", "foo_name", "bar2", "test", "bar", "name", 1L, true, null, LONG },
                { "zap", "name_value", "pow", "zap", "pow", "name", 0L, true, null, LONG },
                { "zap", "name_value", "pow", "zap", "pow", "value", 1L, true, null, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.INDEX_COLUMNS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S index_columns", 0, skipped);
    }
}
