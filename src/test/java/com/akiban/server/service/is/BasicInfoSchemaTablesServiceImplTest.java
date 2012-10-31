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
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.Parameter;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.SQLJJar;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.View;
import com.akiban.ais.util.ChangedTableDescription;
import com.akiban.qp.memoryadapter.MemoryAdapter;
import com.akiban.qp.memoryadapter.MemoryTableFactory;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.TableDefinition;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;

import static com.akiban.qp.memoryadapter.MemoryGroupCursor.GroupScan;
import static com.akiban.server.types.AkType.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class BasicInfoSchemaTablesServiceImplTest {
    private static final String I_S = TableName.INFORMATION_SCHEMA;

    private AkibanInformationSchema ais;
    private BasicInfoSchemaTablesServiceImpl bist;
    private MemoryAdapter adapter;

    @Before
    public void setUp() throws Exception {
        ais = BasicInfoSchemaTablesServiceImpl.createTablesToRegister();
        createTables();
        bist = new BasicInfoSchemaTablesServiceImpl(new MockSchemaManager(ais));
        bist.attachFactories(ais, false);
        adapter = new MemoryAdapter(new Schema(ais), null, null);
    }

    private static void simpleTable(AISBuilder builder, String group, String schema, String table, String parentName, boolean withPk) {
        builder.userTable(schema, table);
        builder.column(schema, table, "id", 0, "INT", null, null, false, false, null, null);
        if(parentName != null) {
            builder.column(schema, table, "pid", 1, "INT", null, null, false, false, null, null);
        }
        if(withPk) {
            builder.index(schema, table, Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
            builder.indexColumn(schema, table, Index.PRIMARY_KEY_CONSTRAINT, "id", 0, true, null);
        }

        if(parentName == null) {
            builder.createGroup(group, schema);
        } else {
            String joinName = table + "/" + parentName;
            builder.joinTables(joinName, schema, parentName, schema, table);
            builder.joinColumns(joinName, schema, parentName, "id", schema, table, "pid");
            builder.addJoinToGroup(group, joinName, 0);
        }
    }

    private void createTables() throws Exception {
        AISBuilder builder = new AISBuilder(ais);

        {
        String schema = "test";
        String table = "foo";
        builder.userTable(schema, table);
        builder.column(schema, table, "c1", 0, "INT", null, null, false, false, null, null);
        builder.column(schema, table, "c2", 1, "DOUBLE", null, null, true, false, null, null);
        builder.createGroup(table, schema);
        builder.addTableToGroup(table, schema, table);
        // no defined pk or indexes
        }

        {
        String schema = "test";
        String table = "bar";
        builder.userTable(schema, table);
        builder.column(schema, table, "col", 0, "BIGINT", null, null, false, false, null, null);
        builder.column(schema, table, "name", 1, "INT", null, null, false, false, null, null);
        builder.index(schema, table, Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(schema, table, Index.PRIMARY_KEY_CONSTRAINT, "col", 0, true, null);
        builder.createGroup(table, schema);

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
        }

        {
        String schema = "zap";
        String table = "pow";
        String indexName = "name_value";
        builder.userTable(schema, table);
        builder.column(schema, table, "name", 0, "VARCHAR", 32L, null, true, false, null, null);
        builder.column(schema, table, "value", 1, "DECIMAL", 10L, 2L, true, false, null, null);
        builder.index(schema, table, indexName, true, Index.UNIQUE_KEY_CONSTRAINT);
        builder.indexColumn(schema, table, indexName, "name", 0, true, null);
        builder.indexColumn(schema, table, indexName, "value", 1, true, null);
        builder.createGroup(table, schema);
        builder.addTableToGroup(table, schema, table);
        // no defined pk
        }

        {
        // Added for bug1019905: Last table only had GFK show up in constraints/key_column_usage if it had a GFK
        String schema = "zzz";
        String table = schema + "1";
        builder.userTable(schema, table);
        builder.column(schema, table, "id", 0, "INT", null, null, false, false, null, null);
        builder.index(schema, table, Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(schema, table, Index.PRIMARY_KEY_CONSTRAINT, "id", 0, true, null);
        builder.createGroup(table, schema);

        String childTable = schema + "2";
        builder.userTable(schema, childTable);
        builder.column(schema, childTable, "id", 0, "INT", null, null, false, false, null, null);
        builder.column(schema, childTable, "one_id", 1, "INT", null, null, true, false, null, null);
        builder.index(schema, childTable, Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(schema, childTable, Index.PRIMARY_KEY_CONSTRAINT, "id", 0, true, null);

        String joinName = childTable + "/" + table;
        builder.joinTables(joinName, schema, table, schema, childTable);
        builder.joinColumns(joinName, schema, table, "id", schema, childTable, "one_id");
        builder.addJoinToGroup(table, joinName, 0);
        }

        {
        // bug1024965: Grouping constraints not in depth order
        /*
        * r
        * |-m
        * | |-b
        * |   |-x
        * |-a
        *   |-w
        */
        String schema = "gco";
        String group = "r";
        simpleTable(builder, group, schema,  "r", null, true);
        simpleTable(builder, group, schema, "m", "r", true);
        simpleTable(builder, group, schema, "b", "m", true);
        simpleTable(builder, group, schema, "x", "b", false);
        simpleTable(builder, group, schema, "a", "r", true);
        simpleTable(builder, group, schema, "w", "a", false);
        }
        {
        /* Sequence testing */
        String schema = "test";
        String sequence = "sequence";
        builder.sequence(schema, sequence, 1, 1, 0, 1000, false);
        sequence = sequence + "1";
        builder.sequence(schema, sequence, 1000, -1, 0, 1000, false);
        
        String table = "seq-table";
        sequence = "_col_sequence";
        builder.userTable(schema, table);
        builder.column(schema, table, "col", 0, "BIGINT", null, null, false, false, null, null);
        builder.index(schema, table, Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(schema, table, Index.PRIMARY_KEY_CONSTRAINT, "col", 0, true, null);
        builder.sequence(schema, sequence, 1, 1, 0, 1000, false);
        builder.columnAsIdentity(schema, table, "col", sequence, true);
        builder.createGroup(table, schema);
        builder.addTableToGroup(table, schema, table);
        }

        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();

        Map<Table, Integer> ordinalMap = new HashMap<Table, Integer>();
        List<UserTable> remainingTables = new ArrayList<UserTable>();
        // Add all roots
        for(UserTable userTable : ais.getUserTables().values()) {
            if(userTable.isRoot()) {
                userTable.getGroup().setTreeName(userTable.getName().getTableName() + "_tree");
                remainingTables.add(userTable);
            }
        }
        while(!remainingTables.isEmpty()) {
            UserTable userTable = remainingTables.remove(remainingTables.size()-1);
            ordinalMap.put(userTable, 0);
            for(Index index : userTable.getIndexesIncludingInternal()) {
                index.computeFieldAssociations(ordinalMap);
                index.setTreeName(index.getIndexName().getName() + "_tree");
            }
            // Add all immediate children
            for(Join join : userTable.getChildJoins()) {
                remainingTables.add(join.getChild());
            }
        }
        for(Group group : ais.getGroups().values()) {
            for(Index index : group.getIndexes()) {
                index.computeFieldAssociations(ordinalMap);
                index.setTreeName(index.getIndexName().getName() + "_tree");
            }
        }

        {
        String schema = "test";
        String view = "voo";
        Map<TableName,Collection<String>> refs = new HashMap<TableName,Collection<String>>();
        refs.put(TableName.create(schema, "foo"), Arrays.asList("c1", "c2"));
        builder.view(schema, view,
                     "CREATE VIEW voo(c1,c2) AS SELECT c2,c1 FROM foo", new Properties(),
                     refs);
        builder.column(schema, view, "c1", 0, "DOUBLE", null, null, true, false, null, null);
        builder.column(schema, view, "c2", 1, "INT", null, null, false, false, null, null);
        }

        builder.sqljJar("test", "ajar", 
                        new URL("https://software.akiban.com/procs/ajar.jar"));

        builder.routine("test", "proc1", "java", Routine.CallingConvention.JAVA);
        builder.parameter("test", "proc1", "n1", Parameter.Direction.IN,
                          "bigint", null, null);
        builder.parameter("test", "proc1", "s1", Parameter.Direction.IN,
                          "varchar", 16L, null);
        builder.parameter("test", "proc1", "n2", Parameter.Direction.IN,
                          "decimal", 10L, 5L);
        builder.parameter("test", "proc1", null, Parameter.Direction.OUT,
                          "varchar", 100L, null);
        builder.routineExternalName("test", "proc1", "test", "ajar",
                                    "com.akiban.procs.Proc1", "call");
    }

    private MemoryTableFactory getFactory(TableName name) {
        UserTable table = ais.getUserTable(name);
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
                
                if(expected == null || actual.isNull()) {
                    Column column = row.rowType().userTable().getColumn(colIndex);
                    if(!Boolean.TRUE.equals(column.getNullable())) {
                        fail(String.format("Expected (%s) or actual (%s) NULL for column (%s) declared NOT NULL",
                                           expected, actual, column));
                    }
                }

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
                } else if(expected instanceof Text) {
                    assertEquals(msg, ((Text)expected).getText(), actual.getText());
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

    static class Text {
        private String text;
        public Text(String text) {
            this.text = text;
        }
        public String getText() {
            return text;
        }
    }

    @Test
    public void schemataScan() {
        final Object[][] expected = {
                { "gco", null, null, null, LONG },
                { "test", null, null, null, LONG },
                { "zap", null, null, null, LONG },
                { "zzz", null, null, null, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.SCHEMATA).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S schemas", 1, skipped);
    }

    @Test
    public void tablesScan() {
        final Object[][] expected = {
                { "gco", "a", "TABLE", LONG, "r_tree", I_S, VARCHAR, I_S, VARCHAR, LONG },
                { "gco", "b", "TABLE", LONG, "r_tree", I_S, VARCHAR, I_S, VARCHAR, LONG },
                { "gco", "m", "TABLE", LONG, "r_tree", I_S, VARCHAR, I_S, VARCHAR, LONG },
                { "gco", "r", "TABLE", LONG, "r_tree", I_S, VARCHAR, I_S, VARCHAR, LONG },
                { "gco", "w", "TABLE", LONG, "r_tree", I_S, VARCHAR, I_S, VARCHAR, LONG },
                { "gco", "x", "TABLE", LONG, "r_tree", I_S, VARCHAR, I_S, VARCHAR, LONG },
                { "test", "bar", "TABLE", LONG, "bar_tree", I_S, VARCHAR, I_S, VARCHAR, LONG },
                { "test", "bar2", "TABLE", LONG, "bar_tree", I_S, VARCHAR, I_S, VARCHAR, LONG },
                { "test", "foo", "TABLE", LONG, "foo_tree", I_S, VARCHAR, I_S, VARCHAR, LONG },
                { "test", "seq-table", "TABLE", LONG, "seq-table_tree", I_S, VARCHAR, I_S, VARCHAR, LONG}, 
                { "zap", "pow", "TABLE", LONG, "pow_tree", I_S, VARCHAR, I_S, VARCHAR, LONG },
                { "zzz", "zzz1", "TABLE", LONG, "zzz1_tree", I_S, VARCHAR, I_S, VARCHAR, LONG },
                { "zzz", "zzz2", "TABLE", LONG, "zzz1_tree", I_S, VARCHAR, I_S, VARCHAR, LONG },
                { "test", "voo", "VIEW", null, null, null, null, null, null, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.TABLES).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S tables", 17, skipped);
    }

    @Test
    public void columnsScan() {
        final Object[][] expected = {
                { "gco", "a", "id", 0L, "int", false, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "gco", "a", "pid", 1L, "int", false, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "gco", "b", "id", 0L, "int", false, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "gco", "b", "pid", 1L, "int", false, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "gco", "m", "id", 0L, "int", false, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "gco", "m", "pid", 1L, "int", false, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "gco", "r", "id", 0L, "int", false, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "gco", "w", "id", 0L, "int", false, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "gco", "w", "pid", 1L, "int", false, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "gco", "x", "id", 0L, "int", false, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "gco", "x", "pid", 1L, "int", false, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "test", "bar", "col", 0L, "bigint", false, 8L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "test", "bar", "name", 1L, "int", false, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "test", "bar2", "foo", 0L, "int", true, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "test", "bar2", "pid", 1L, "int", true, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "test", "foo", "c1", 0L, "int", false, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "test", "foo", "c2", 1L, "double", true, 8L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "test", "seq-table", "col", 0L, "bigint", false, 8L, null, null, 0L, null, null, null, null, null, "test", "_col_sequence", "BY DEFAULT", LONG}, 
                { "zap", "pow", "name", 0L, "varchar", true, 32L, null, null, 1L, null, I_S, VARCHAR, I_S, VARCHAR, null, null, null, LONG },
                { "zap", "pow", "value", 1L, "decimal", true, 5L, 10L, 2L, 0L, null, null, null, null, null, null, null, null, LONG },
                { "zzz", "zzz1", "id", 0L, "int", false, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "zzz", "zzz2", "id", 0L, "int", false, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "zzz", "zzz2", "one_id", 1L, "int", true, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "test", "voo", "c1", 0L, "double", true, 8L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
                { "test", "voo", "c2", 1L, "int", false, 4L, null, null, 0L, null, null, null, null, null, null, null, null, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.COLUMNS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S columns", 127, skipped);
    }

    @Test
    public void tableConstraintsScan() {
        final Object[][] expected = {
                { "gco", "a", "a/r", "GROUPING", LONG },
                { "gco", "a", "PRIMARY", "PRIMARY KEY", LONG },
                { "gco", "b", "b/m", "GROUPING", LONG },
                { "gco", "b", "PRIMARY", "PRIMARY KEY", LONG },
                { "gco", "m", "m/r", "GROUPING", LONG },
                { "gco", "m", "PRIMARY", "PRIMARY KEY", LONG },
                { "gco", "r", "PRIMARY", "PRIMARY KEY", LONG },
                { "gco", "w", "w/a", "GROUPING", LONG },
                { "gco", "x", "x/b", "GROUPING", LONG },
                { "test", "bar", "PRIMARY", "PRIMARY KEY", LONG },
                { "test", "bar2", "bar2/bar", "GROUPING", LONG },
                { "test", "seq-table", "PRIMARY", "PRIMARY KEY", LONG},
                { "zap", "pow", "name_value", "UNIQUE", LONG },
                { "zzz", "zzz1", "PRIMARY", "PRIMARY KEY", LONG },
                { "zzz", "zzz2", "zzz2/zzz1", "GROUPING", LONG },
                { "zzz", "zzz2", "PRIMARY", "PRIMARY KEY", LONG },
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
                { "gco", "r", "gco", "r", "gco.r", 0L, null, null, null, null, LONG },
                { "gco", "r", "gco", "m", "gco.r/gco.m", 1L, "m/r", "gco", "r", "PRIMARY", LONG },
                { "gco", "r", "gco", "b", "gco.r/gco.m/gco.b", 2L, "b/m", "gco", "m", "PRIMARY", LONG },
                { "gco", "r", "gco", "x", "gco.r/gco.m/gco.b/gco.x", 3L, "x/b", "gco", "b", "PRIMARY", LONG },
                { "gco", "r", "gco", "a", "gco.r/gco.a", 1L, "a/r", "gco", "r", "PRIMARY", LONG },
                { "gco", "r", "gco", "w", "gco.r/gco.a/gco.w", 2L, "w/a", "gco", "a", "PRIMARY", LONG },
                { "test", "bar", "test", "bar", "test.bar", 0L, null, null, null, null, LONG },
                { "test", "bar", "test", "bar2", "test.bar/test.bar2", 1L, "bar2/bar", "test", "bar", "PRIMARY", LONG },
                { "test", "foo", "test", "foo", "test.foo", 0L, null, null, null, null, LONG },
                { "test", "seq-table", "test", "seq-table", "test.seq-table", 0L, null, null, null, null, LONG},
                { "zap", "pow", "zap", "pow", "zap.pow", 0L, null, null, null, null, LONG },
                { "zzz", "zzz1", "zzz", "zzz1", "zzz.zzz1", 0L, null, null, null, null, LONG },
                { "zzz", "zzz1", "zzz", "zzz2", "zzz.zzz1/zzz.zzz2", 1L, "zzz2/zzz1", "zzz", "zzz1", "PRIMARY", LONG },
        };

        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.GROUPING_CONSTRAINTS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S grouping_constraints", 17, skipped);
    }

    @Test
    public void keyColumnUsageScan() {    
        final Object[][] expected = {
                {"gco", "a", "a/r", "pid", 0L, 0L, LONG },
                {"gco", "a", "PRIMARY", "id", 0L, null, LONG },
                {"gco", "b", "b/m", "pid", 0L, 0L, LONG },
                {"gco", "b", "PRIMARY", "id", 0L, null, LONG },
                {"gco", "m", "m/r", "pid", 0L, 0L, LONG },
                {"gco", "m", "PRIMARY", "id", 0L, null, LONG },
                {"gco", "r", "PRIMARY", "id", 0L, null, LONG },
                {"gco", "w", "w/a", "pid", 0L, 0L, LONG },
                {"gco", "x", "x/b", "pid", 0L, 0L, LONG },
                { "test", "bar", "PRIMARY", "col", 0L, null, LONG },
                { "test", "bar2", "bar2/bar", "pid", 0L, 0L, LONG },
                { "test", "seq-table", "PRIMARY", "col", 0L, null, LONG}, 
                { "zap", "pow", "name_value", "name", 0L, null, LONG },
                { "zap", "pow", "name_value", "value", 1L, null, LONG },
                { "zzz", "zzz1", "PRIMARY", "id", 0L, null, LONG },
                { "zzz", "zzz2", "zzz2/zzz1", "one_id", 0L, 0L, LONG },
                { "zzz", "zzz2", "PRIMARY", "id", 0L, null, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.KEY_COLUMN_USAGE).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S key_column_usage", 0, skipped);
    }

    @Test
    public void indexesScan() {
        final Object[][] expected = {
                { "gco", "a", "PRIMARY", "PRIMARY", LONG, "PRIMARY_tree", "PRIMARY", true, null, null, LONG },
                { "gco", "b", "PRIMARY", "PRIMARY", LONG, "PRIMARY_tree", "PRIMARY", true, null, null, LONG },
                { "gco", "m", "PRIMARY", "PRIMARY", LONG, "PRIMARY_tree", "PRIMARY", true, null, null, LONG },
                { "gco", "r", "PRIMARY", "PRIMARY", LONG, "PRIMARY_tree", "PRIMARY", true, null, null, LONG },
                { "test", "bar", "PRIMARY", "PRIMARY", LONG, "PRIMARY_tree", "PRIMARY", true, null, null, LONG },
                { "test", "bar2", "foo_name", null, LONG, "foo_name_tree", "INDEX", false, "RIGHT", null, LONG },
                { "test", "seq-table", "PRIMARY", "PRIMARY", LONG, "PRIMARY_tree", "PRIMARY", true, null, null, LONG},
                { "zap", "pow", "name_value", "name_value", LONG, "name_value_tree", "UNIQUE", true, null, null, LONG },
                { "zzz", "zzz1", "PRIMARY", "PRIMARY", LONG, "PRIMARY_tree", "PRIMARY", true, null, null, LONG },
                { "zzz", "zzz2", "PRIMARY", "PRIMARY", LONG, "PRIMARY_tree", "PRIMARY", true, null, null, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.INDEXES).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S indexes", 0, skipped);
    }

    @Test
    public void indexColumnsScan() {
        final Object[][] expected = {
                { "gco", "PRIMARY", "a", "gco", "a", "id", 0L, true, null, LONG },
                { "gco", "PRIMARY", "b", "gco", "b", "id", 0L, true, null, LONG },
                { "gco", "PRIMARY", "m", "gco", "m", "id", 0L, true, null, LONG },
                { "gco", "PRIMARY", "r", "gco", "r", "id", 0L, true, null, LONG },
                { "test", "PRIMARY", "bar", "test", "bar", "col", 0L, true, null, LONG },
                { "test", "foo_name", "bar2", "test", "bar2", "foo", 0L, true, null, LONG },
                { "test", "foo_name", "bar2", "test", "bar", "name", 1L, true, null, LONG },
                { "test", "PRIMARY", "seq-table", "test", "seq-table", "col", 0L, true, null, LONG}, 
                { "zap", "name_value", "pow", "zap", "pow", "name", 0L, true, null, LONG },
                { "zap", "name_value", "pow", "zap", "pow", "value", 1L, true, null, LONG },
                { "zzz", "PRIMARY", "zzz1", "zzz", "zzz1", "id", 0L, true, null, LONG },
                { "zzz", "PRIMARY", "zzz2", "zzz", "zzz2", "id", 0L, true, null, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.INDEX_COLUMNS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S index_columns", 0, skipped);
    }

    @Test
    public void sequencesScan() {
        final Object[][] expected = {
                {"test", "_col_sequence", "test._col_sequence", 1L, 1L, 0L, 1000L, false, LONG},
                {"test", "sequence", "test.sequence", 1L, 1L, 0L, 1000L, false, LONG },
                {"test", "sequence1", "test.sequence1", 1000L, -1L, 0L, 1000L, false, LONG},
        };
        GroupScan scan = getFactory (BasicInfoSchemaTablesServiceImpl.SEQUENCES).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S sequences", 0, skipped);
    }

    @Test
    public void viewsScan() {
        final Object[][] expected = {
                { "test", "voo", new Text("CREATE VIEW voo(c1,c2) AS SELECT c2,c1 FROM foo"), false, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.VIEWS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S views", 0, skipped);
    }

    @Test
    public void viewTableUsageScan() {
        final Object[][] expected = {
                { "test", "voo", "test", "foo", LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.VIEW_TABLE_USAGE).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S views", 0, skipped);
    }

    @Test
    public void viewColumnUsageScan() {
        final Object[][] expected = {
                { "test", "voo", "test", "foo", "c1", LONG },
                { "test", "voo", "test", "foo", "c2", LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.VIEW_COLUMN_USAGE).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S views", 0, skipped);
    }

    @Test
    public void routinesScan() {
        final Object[][] expected = {
            { "test", "proc1", "PROCEDURE", null, "com.akiban.procs.Proc1.call", "java", "JAVA", "NO", null, "YES", 0L, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.ROUTINES).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S routines", 0, skipped);
    }

    @Test
    public void parametersScan() {
        final Object[][] expected = {
            { "test", "proc1", "n1", 1L, "bigint", null, null, null, "IN", "NO", LONG },
            { "test", "proc1", "s1", 2L, "varchar", 16L, null, null, "IN", "NO", LONG },
            { "test", "proc1", "n2", 3L, "decimal", null, 10L, 5L, "IN", "NO", LONG },
            { "test", "proc1", null, 4L, "varchar", 100L, null, null, "OUT", "NO", LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.PARAMETERS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S parameters", 0, skipped);
    }

    @Test
    public void jarsScan() {
        final Object[][] expected = {
            { "test", "ajar", "https://software.akiban.com/procs/ajar.jar", LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.JARS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S jars", 0, skipped);
    }

    @Test
    public void routineJarUsageScan() {
        final Object[][] expected = {
                { "test", "proc1", "test", "ajar", LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.ROUTINE_JAR_USAGE).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S routines", 0, skipped);
    }


    private static class MockSchemaManager implements SchemaManager {
        final AkibanInformationSchema ais;

        public MockSchemaManager(AkibanInformationSchema ais) {
            this.ais = ais;
        }

        @Override
        public AkibanInformationSchema getAis(Session session) {
            return ais;
        }


        @Override
        public TableName registerStoredInformationSchemaTable(UserTable newTable, int version) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableName registerMemoryInformationSchemaTable(UserTable newTable, MemoryTableFactory factory) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void unRegisterMemoryInformationSchemaTable(TableName tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableName createTableDefinition(Session session, UserTable newTable) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameTable(Session session, TableName currentName, TableName newName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<Index> createIndexes(Session session, Collection<? extends Index> indexes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropIndexes(Session session, Collection<? extends Index> indexes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropTableDefinition(Session session, String schemaName, String tableName, DropBehavior dropBehavior) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterTableDefinitions(Session session, Collection<ChangedTableDescription> alteredTables) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableDefinition getTableDefinition(Session session, TableName tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedMap<String, TableDefinition> getTableDefinitions(Session session, String schemaName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> schemaStrings(Session session, boolean withISTables) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createView(Session session, View view) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropView(Session session, TableName viewName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createSequence(Session session, Sequence sequence) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropSequence(Session session, Sequence sequence) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createRoutine(Session session, Routine routine) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropRoutine(Session session, TableName routineName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createSQLJJar(Session session, SQLJJar sqljJar) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void replaceSQLJJar(Session session, SQLJJar sqljJar) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropSQLJJar(Session session, TableName jarName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void registerSystemRoutine(Routine routine) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void unRegisterSystemRoutine(TableName routineName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean treeRemovalIsDelayed() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void treeWasRemoved(Session session, String schemaName, String treeName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getTreeNames() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getOldestActiveAISGeneration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasTableChanged(Session session, int tableID) {
            throw new UnsupportedOperationException();
        }
    }
}
