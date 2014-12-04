/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.service.is;

import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.DefaultNameGenerator;
import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.ais.model.View;
import com.foundationdb.ais.util.ChangedTableDescription;
import com.foundationdb.qp.memoryadapter.MemoryAdapter;
import com.foundationdb.qp.memoryadapter.MemoryTableFactory;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.TableChanges.ChangeSet;
import com.foundationdb.server.store.format.DummyStorageFormatRegistry;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.server.types.service.TestTypesRegistry;
import com.foundationdb.server.types.service.TypesRegistry;
import com.foundationdb.server.types.value.ValueSource;
import com.persistit.Key;

import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.foundationdb.qp.memoryadapter.MemoryGroupCursor.GroupScan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class BasicInfoSchemaTablesServiceImplTest {
    private static final String I_S = TableName.INFORMATION_SCHEMA;

    private AkibanInformationSchema ais;
    private TypesRegistry typesRegistry;
    private TypesTranslator typesTranslator;
    private SchemaManager schemaManager;
    private NameGenerator nameGenerator;
    private BasicInfoSchemaTablesServiceImpl bist;
    private MemoryAdapter adapter;

    @Before
    public void setUp() throws Exception {
        typesRegistry = TestTypesRegistry.MCOMPAT;
        typesTranslator = MTypesTranslator.INSTANCE;
        ais = BasicInfoSchemaTablesServiceImpl.createTablesToRegister(typesTranslator);
        schemaManager = new MockSchemaManager(ais, typesRegistry, typesTranslator);
        nameGenerator = new DefaultNameGenerator();
        createTables();
        bist = new BasicInfoSchemaTablesServiceImpl(schemaManager, null, null);
        bist.attachFactories(ais);
        adapter = new MemoryAdapter(null, null);
    }

    private static void simpleTable(TestAISBuilder builder, String group, String schema, String table, String parentName, boolean withPk) {
        builder.table(schema, table);
        builder.column(schema, table, "id", 0, "MCOMPAT", "INT", false);
        if(parentName != null) {
            builder.column(schema, table, "pid", 1, "MCOMPAT", "INT", false);
        }
        if(withPk) {
            builder.pk(schema, table);
            builder.indexColumn(schema, table, Index.PRIMARY, "id", 0, true, null);
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
        TestAISBuilder builder = new TestAISBuilder(ais, nameGenerator,
                                                    schemaManager.getTypesRegistry(), schemaManager.getStorageFormatRegistry());

        {
        String schema = "test";
        String table = "foo";
        builder.table(schema, table);
        builder.column(schema, table, "c1", 0, "MCOMPAT", "INT", false);
        builder.column(schema, table, "c2", 1, "MCOMPAT", "DOUBLE", true);
        builder.createGroup(table, schema);
        builder.addTableToGroup(table, schema, table);
        // no defined pk or indexes
        }

        {
        String schema = "test";
        String table = "bar";
        builder.table(schema, table);
        builder.column(schema, table, "col", 0, "MCOMPAT", "BIGINT", false);
        builder.column(schema, table, "name", 1, "MCOMPAT", "INT", false);
        builder.pk(schema, table);
        builder.indexColumn(schema, table, Index.PRIMARY, "col", 0, true, null);
        builder.createGroup(table, schema);

        schema = "test";
        String childTable = table + "2";
        String indexName = "foo_name";
        builder.table(schema, childTable);
        builder.column(schema, childTable, "foo", 0, "MCOMPAT", "INT", true);
        builder.column(schema, childTable, "pid", 1, "MCOMPAT", "INT", true);

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
        builder.table(schema, table);
        builder.column(schema, table, "name", 0, "MCOMPAT", "VARCHAR", 32L, null, true);
        builder.column(schema, table, "value", 1, "MCOMPAT", "DECIMAL", 10L, 2L, true);
        builder.unique(schema, table, indexName);
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
        builder.table(schema, table);
        builder.column(schema, table, "id", 0, "MCOMPAT", "INT", false);
        builder.pk(schema, table);
        builder.indexColumn(schema, table, Index.PRIMARY, "id", 0, true, null);
        builder.createGroup(table, schema);

        String childTable = schema + "2";
        builder.table(schema, childTable);
        builder.column(schema, childTable, "id", 0, "MCOMPAT", "INT", false);
        builder.column(schema, childTable, "one_id", 1, "MCOMPAT", "INT", true);
        builder.pk(schema, childTable);
        builder.indexColumn(schema, childTable, Index.PRIMARY, "id", 0, true, null);

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
        builder.table(schema, table);
        builder.column(schema, table, "col", 0, "MCOMPAT", "BIGINT", false);
        builder.pk(schema, table);
        builder.indexColumn(schema, table, Index.PRIMARY, "col", 0, true, null);
        builder.sequence(schema, sequence, 1, 1, 0, 1000, false);
        builder.columnAsIdentity(schema, table, "col", sequence, true);
        builder.createGroup(table, schema);
        builder.addTableToGroup(table, schema, table);
        }
        
        {
        String schema = "test";
        String table = "defaults";
        builder.table(schema, table);
        builder.column(schema, table, "col1", 0, "MCOMPAT", "VARCHAR", 10L, null, false, "fred", null);
        builder.column(schema, table, "col2", 1, "MCOMPAT", "VARCHAR", 10L, null, false, "", null);
        builder.column(schema, table, "col3", 2, "MCOMPAT", "BIGINT", null, null, false, "0", null);
        builder.column(schema, table, "col4", 3, "MCOMPAT", "DATE",   null, null, false, null, "current_date");
        builder.column(schema, table, "col5", 4, "MCOMPAT", "DECIMAL", 11L, 2L,  false, "5.5", null);
        builder.column(schema, table, "col6", 5, "MCOMPAT", "VARBINARY", 15L, null, false, null, null);
        builder.createGroup(table, schema);
        builder.addTableToGroup(table, schema, table);
        }

        {
            String schema = "test";
            String table = "parent";
            builder.table(schema, table);
            builder.column(schema, table, "col1", 0, "MCOMPAT", "BIGINT", null, false, null, null);
            builder.pk(schema, table);
            builder.indexColumn(schema, table, Index.PRIMARY, "col1", 0, true, null);
            builder.createGroup(table, schema);
            builder.addTableToGroup(table, schema, table);
            
            table = "child";
            builder.table(schema, table);
            builder.column(schema, table, "col1", 0, "MCOMPAT", "BIGINT", null, false, null, null);
            builder.column(schema, table, "col2", 1, "MCOMPAT", "BIGINT", null, true, null, null);
            builder.pk(schema, table);
            builder.indexColumn(schema, table, Index.PRIMARY, "col1", 0, true, null);
            builder.foreignKey(schema, "child", Arrays.asList("col2"), schema, "parent", Arrays.asList("col1"), ForeignKey.Action.RESTRICT, ForeignKey.Action.RESTRICT, true, true, "fkey_parent");
            builder.createGroup(table, schema);
            builder.addTableToGroup(table, schema, table);
            
        }
        
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();

        Map<Table, Integer> ordinalMap = new HashMap<>();
        List<Table> remainingTables = new ArrayList<>();
        // Add all roots
        for(Table table : ais.getTables().values()) {
            if(table.isRoot()) {
                remainingTables.add(table);
            }
        }
        while(!remainingTables.isEmpty()) {
            Table table = remainingTables.remove(remainingTables.size()-1);
            ordinalMap.put(table, 0);
            for(Index index : table.getIndexesIncludingInternal()) {
                index.computeFieldAssociations(ordinalMap);
            }
            // Add all immediate children
            for(Join join : table.getChildJoins()) {
                remainingTables.add(join.getChild());
            }
        }
        for(Group group : ais.getGroups().values()) {
            for(Index index : group.getIndexes()) {
                index.computeFieldAssociations(ordinalMap);
            }
        }

        {
        String schema = "test";
        String view = "voo";
        Map<TableName,Collection<String>> refs = new HashMap<>();
        refs.put(TableName.create(schema, "foo"), Arrays.asList("c1", "c2"));
        builder.view(schema, view,
                     "CREATE VIEW voo(c1,c2) AS SELECT c2,c1 FROM foo", new Properties(),
                     refs);
        builder.column(schema, view, "c1", 0, "MCOMPAT", "DOUBLE", true);
        builder.column(schema, view, "c2", 1, "MCOMPAT", "INT", false);
        }

        builder.sqljJar("test", "ajar", 
                        new URL("https://example.com/procs/ajar.jar"));

        builder.routine("test", "proc1", "java", Routine.CallingConvention.JAVA);
        builder.parameter("test", "proc1", "n1", Parameter.Direction.IN,
                          "MCOMPAT", "bigint", null, null);
        builder.parameter("test", "proc1", "s1", Parameter.Direction.IN,
                          "MCOMPAT", "varchar", 16L, null);
        builder.parameter("test", "proc1", "n2", Parameter.Direction.IN,
                          "MCOMPAT", "decimal", 10L, 5L);
        builder.parameter("test", "proc1", null, Parameter.Direction.OUT,
                          "MCOMPAT", "varchar", 100L, null);
        builder.routineExternalName("test", "proc1", "test", "ajar",
                                    "com.foundationdb.procs.Proc1", "call");
    }

    private MemoryTableFactory getFactory(TableName name) {
        Table table = ais.getTable(name);
        assertNotNull("No such table: " + name, table);
        MemoryTableFactory factory = MemoryAdapter.getMemoryTableFactory(table);
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
                final ValueSource actual = row.value(colIndex);
                
                if(expected == null || actual.isNull()) {
                    Column column = row.rowType().table().getColumn(colIndex);
                    if(!Boolean.TRUE.equals(column.getNullable())) {
                        fail(String.format("Expected (%s) or actual (%s) NULL for column (%s) declared NOT NULL",
                                           expected, actual, column));
                    }
                }

                if(expected == null) {
                    assertEquals(msg + " isNull", true, actual.isNull());
                } else if(expected instanceof TInstance) {
                    assertEquals(msg + " (type only)", expected, actual.getType());
                } else if(expected instanceof String) {
                    if(colIndex == 1 && actual.getString().equals(I_S)) {
                        --rowIndex;
                        ++skippedRows;
                        break;
                    }
                    assertEquals(msg, expected, actual.getString());
   
                } else if(expected instanceof Integer) {
                    assertEquals(msg, expected, actual.getInt32());
                } else if(expected instanceof Long) {
                    assertEquals(msg, expected, actual.getInt64());
                } else if(expected instanceof Boolean) {
                    assertEquals(msg, (Boolean)expected ? "YES" : "NO", actual.getString());
                } else if(expected instanceof Text) {
                    assertEquals(msg, ((Text)expected).getText(), actual.getString());
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

    private static final TInstance LONG = MNumeric.BIGINT.instance(false);
    private static final TInstance LONG_NULL = MNumeric.BIGINT.instance(true);
    private static final TInstance VARCHAR = MString.VARCHAR.instance(128,true);

    @Test
    public void schemataScan() {
        final Object[][] expected = {
                { null, "gco",  null, null, null, null, null, null, null, null, LONG },
                { null, "test", null, null, null, null, null, null, null, null, LONG },
                { null, "zap",  null, null, null, null, null, null, null, null, LONG },
                { null, "zzz",  null, null, null, null, null, null, null, null, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.SCHEMATA).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S schemas", 1, skipped);
    }

    @Test
    public void tablesScan() {
        final Object[][] expected = {
                { null, "gco", "a", "TABLE", null, null, "YES", "NO", null, null, null, VARCHAR, null, null, VARCHAR, LONG_NULL, null, "gco.r", VARCHAR, LONG },
                { null, "gco", "b", "TABLE", null, null, "YES", "NO", null, null, null, VARCHAR, null, null, VARCHAR, LONG_NULL, null, "gco.r", VARCHAR, LONG },
                { null, "gco", "m", "TABLE", null, null, "YES", "NO", null, null, null, VARCHAR, null, null, VARCHAR, LONG_NULL, null, "gco.r", VARCHAR, LONG },
                { null, "gco", "r", "TABLE", null, null, "YES", "NO", null, null, null, VARCHAR, null, null, VARCHAR, LONG_NULL, null, "gco.r", VARCHAR, LONG },
                { null, "gco", "w", "TABLE", null, null, "YES", "NO", null, null, null, VARCHAR, null, null, VARCHAR, LONG_NULL, null, "gco.r", VARCHAR, LONG },
                { null, "gco", "x", "TABLE", null, null, "YES", "NO", null, null, null, VARCHAR, null, null, VARCHAR, LONG_NULL, null, "gco.r", VARCHAR, LONG },
                { null, "test", "bar", "TABLE", null, null, "YES", "NO", null, null,  null, VARCHAR, null, null, VARCHAR,LONG_NULL, null, "test.bar", VARCHAR, LONG },
                { null, "test", "bar2", "TABLE", null, null, "YES", "NO", null, null, null, VARCHAR, null, null, VARCHAR, LONG_NULL, null, "test.bar", VARCHAR, LONG },
                { null, "test", "child", "TABLE", null, null, "YES", "NO", null, null, null, VARCHAR, null, null, VARCHAR, LONG_NULL, null, "test.child", VARCHAR, LONG },
                { null, "test", "defaults", "TABLE", null, null, "YES", "NO", null, null,  null, VARCHAR, null, null, VARCHAR, LONG_NULL, null, "test.defaults", VARCHAR, LONG},
                { null, "test", "foo", "TABLE",  null, null, "YES", "NO", null, null, null, VARCHAR, null, null, VARCHAR, LONG_NULL, null, "test.foo", VARCHAR, LONG },
                { null, "test", "parent", "TABLE", null, null, "YES", "NO", null, null, null, VARCHAR, null, null, VARCHAR, LONG_NULL, null, "test.parent", VARCHAR, LONG },
                { null, "test", "seq-table", "TABLE",  null, null, "YES", "NO", null, null, null, VARCHAR, null, null, VARCHAR, LONG_NULL, null, "test.seq-table", VARCHAR, LONG},
                { null, "zap", "pow", "TABLE",  null, null, "YES", "NO", null, null, null, VARCHAR, null, null, VARCHAR, LONG_NULL, null, "zap.pow", VARCHAR, LONG },
                { null, "zzz", "zzz1", "TABLE", null, null, "YES", "NO", null, null, null, VARCHAR, null, null, VARCHAR, LONG_NULL, null, "zzz.zzz1", VARCHAR, LONG },
                { null, "zzz", "zzz2", "TABLE", null, null, "YES", "NO", null, null, null, VARCHAR, null, null, VARCHAR, LONG_NULL, null, "zzz.zzz1", VARCHAR, LONG },
                { null, "test", "voo", "VIEW",  null, null, "NO", "NO", null, null, null, null, null, null, null,   null,null,null, VARCHAR,  LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.TABLES).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S tables", 22, skipped);
    }

    @Test
    public void columnsScan() {
        final Object[][] expected = {
                { null, "gco", "a", "id", 0L,  null, false, "INT", null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES",  null, null, null, LONG},
                { null, "gco", "a", "pid", 1L, null, false, "INT", null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "gco", "b", "id", 0L,  null, false, "INT", null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "gco", "b", "pid", 1L, null, false, "INT", null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "gco", "m", "id", 0L,  null, false, "INT", null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "gco", "m", "pid", 1L, null, false, "INT", null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "gco", "r", "id", 0L,  null, false, "INT", null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "gco", "w", "id", 0L,  null, false, "INT",       null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "gco", "w", "pid", 1L, null, false, "INT",       null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "gco", "x", "id", 0L,  null, false, "INT",       null, null, null, null, null,   null, null, null, null, null, null,  
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
          /*10*/{ null, "gco", "x", "pid", 1L, null, false, "INT",       null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "test", "bar", "col", 0L, null, false, "BIGINT", null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "test", "bar", "name", 1L, null, false, "INT",   null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",   "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "test", "bar2", "foo", 0L, null, true, "INT",   null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",   "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "test", "bar2", "pid", 1L, null, true, "INT",   null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,   null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "test", "child", "col1", 0L, null, false, "BIGINT", null, null, null, null, null,   null, null, null, null, null, null,   
                             null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "test", "child", "col2", 1L, null, true, "BIGINT", null, null, null, null, null,   null, null, null, null, null, null,   
                             null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "test", "defaults", "col1", 0L, "fred", false, "VARCHAR", 10L, 40L, null, null, null,     null, null, VARCHAR, null, null, VARCHAR,
                         null, null, null, null, null, null, null, null, null,   null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "test", "defaults", "col2", 1L, "",  false, "VARCHAR",    10L, 40L, null, null, null,     null, null, VARCHAR, null, null, VARCHAR,
                         null, null, null, null, null, null, null, null, null,   null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "test", "defaults", "col3", 2L, "0", false, "BIGINT",     null, null, null, null, null,   null, null, null, null, null, null,           
                         null, null, null, null, null, null, null, null, null,   null, "NO", "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "test", "defaults", "col4", 3L, "current_date()", false, "DATE",  null, null, null, null, null,   null, null, null, null, null, null,  
                         null, null, null, null, null, null, null, null, null,   null, "NO", "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
         /*20*/ { null, "test", "defaults", "col5", 4L, "5.5", false, "DECIMAL",          null, null, 11L, 10L, 2L,       null, null, null, null, null, null,
                         null, null, null, null, null, null, null, null, null,   null, "NO", "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "test", "defaults", "col6", 5L, null, false, "VARBINARY",         15L, 15L, null, null, null,       null, null, null, null, null, null,
                         null, null, null, null, null, null, null, null, null,   null, "NO", "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "test", "foo", "c1", 0L, null, false, "INT",                      null, null, null, null, null,   null, null, null, null, null, null, 
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES",  null, null, null, LONG},
                { null, "test", "foo", "c2", 1L, null, true, "DOUBLE",                   null, null, null, null, null,   null, null, null, null, null, null,  
                         null, null, null, null, null, null, null, null, null,  null,  "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "test", "parent", "col1", 0L, null, false, "BIGINT", null, null, null, null, null,   null, null, null, null, null, null,   
                          null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "test", "seq-table", "col", 0L, null, false, "BIGINT",            null, null, null, null, null,   null, null, null, null, null, null,  
                         null, null, null, null, null, null, null, null, null,  null, "NO", "YES", "BY DEFAULT", 1L, 1L, 0L, 1000L, "NO",  "NO", null, "YES", null, "test", "_col_sequence",LONG}, 
                { null, "zap", "pow", "name",  0L, null, true,   "VARCHAR",              32L, 128L, null, null, null,     null, null, VARCHAR, null, null, VARCHAR,
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES",  null, null, null, LONG},
                { null, "zap", "pow", "value", 1L, null, true,    "DECIMAL",  null, null, 10L, 10L, 2L,       null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "zzz", "zzz1", "id", 0L,   null, false,   "INT",      null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "zzz", "zzz2", "id", 0L,   null, false,  "INT",       null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
         /*30*/ { null, "zzz", "zzz2", "one_id", 1L, null, true, "INT",      null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "test", "voo", "c1", 0L,   null, true,   "DOUBLE",   null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
                { null, "test", "voo", "c2", 1L,   null, false,  "INT",       null, null, null, null, null,   null, null, null, null, null, null,   
                         null, null, null, null, null, null, null, null, null,  null, "NO",  "NO", null, null, null, null, null, null,  "NO", null, "YES", null, null, null, LONG},
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.COLUMNS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S columns", 245, skipped);
    }

    @Test
    public void tableConstraintsScan() {
        final Object[][] expected = {
                { null, "gco", "a/r",       null, "gco", "a", "GROUPING","NO", "NO", "YES", LONG },
                { null, "gco", "a_pkey", null, "gco", "a", "PRIMARY KEY", "NO", "NO", "YES",LONG },
                { null, "gco", "b/m",       null, "gco", "b", "GROUPING", "NO", "NO", "YES",LONG },
                { null, "gco", "b_pkey", null, "gco", "b", "PRIMARY KEY", "NO", "NO", "YES", LONG },
                { null, "gco", "m/r",       null, "gco", "m", "GROUPING", "NO", "NO", "YES", LONG },
                { null, "gco", "m_pkey", null, "gco", "m", "PRIMARY KEY", "NO", "NO", "YES", LONG },
                { null, "gco", "r_pkey", null, "gco", "r", "PRIMARY KEY", "NO", "NO", "YES", LONG },
                { null, "gco", "w/a",       null, "gco", "w", "GROUPING", "NO", "NO", "YES", LONG },
                { null, "gco", "x/b",       null, "gco", "x", "GROUPING", "NO", "NO", "YES", LONG },
                { null, "test", "bar_pkey",null, "test", "bar", "PRIMARY KEY", "NO", "NO", "YES", LONG },
                { null, "test", "bar2/bar",   null, "test", "bar2","GROUPING", "NO", "NO", "YES", LONG },
                { null, "test", "child_pkey", null, "test", "child", "PRIMARY KEY", "NO", "NO", "YES", LONG},
                { null, "test", "fkey_parent", null, "test", "child", "FOREIGN KEY", "YES", "YES", "YES", LONG},
                { null, "test", "parent_pkey", null, "test", "parent", "PRIMARY KEY", "NO", "NO", "YES", LONG},
                { null, "test", "seq-table_pkey", null, "test", "seq-table", "PRIMARY KEY", "NO", "NO", "YES", LONG},
                { null, "zap", "pow_ukey", null, "zap", "pow", "UNIQUE", "NO", "NO", "YES", LONG },
                { null, "zzz", "zzz1_pkey",   null, "zzz", "zzz1", "PRIMARY KEY", "NO", "NO", "YES", LONG },
                { null, "zzz", "zzz2/zzz1",      null, "zzz", "zzz2", "GROUPING", "NO", "NO", "YES", LONG },
                { null, "zzz", "zzz2_pkey",   null, "zzz", "zzz2", "PRIMARY KEY", "NO", "NO", "YES", LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.TABLE_CONSTRAINTS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S constraints", 0, skipped);
    }

    @Test
    public void referentialConstraintsScan() {
        final Object[][] expected = {
                {null, "test", "fkey_parent", null, "test", "parent_pkey", "SIMPLE", "RESTRICT", "RESTRICT", LONG},
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.REFERENTIAL_CONSTRAINTS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S referential_constraints", 0, skipped);
    }

    @Test
    public void groupingConstraintsScan() {
        final Object[][] expected = {
                { null, "gco", "r", null, "gco", "r", "gco.r", 0L, null, null, null, null, LONG },
                { null, "gco", "r", null, "gco", "m", "gco.r/gco.m", 1L, "m/r", null, "gco", "r_pkey", LONG },
                { null, "gco", "r", null, "gco", "b", "gco.r/gco.m/gco.b", 2L, "b/m", null, "gco", "m_pkey", LONG },
                { null, "gco", "r", null, "gco", "x", "gco.r/gco.m/gco.b/gco.x", 3L, "x/b", null, "gco", "b_pkey", LONG },
                { null, "gco", "r", null, "gco", "a", "gco.r/gco.a", 1L, "a/r", null, "gco", "r_pkey", LONG },
                { null, "gco", "r", null, "gco", "w", "gco.r/gco.a/gco.w", 2L, "w/a", null, "gco", "a_pkey", LONG },
                { null, "test", "bar", null, "test", "bar", "test.bar", 0L, null, null, null,null,  LONG },
                { null, "test", "bar", null, "test", "bar2", "test.bar/test.bar2", 1L, "bar2/bar", null, "test", "bar_pkey", LONG },
                { null, "test", "child", null, "test", "child", "test.child", 0L, null, null, null, null, LONG},
                { null, "test", "defaults", null, "test", "defaults", "test.defaults", 0L, null, null, null, null, LONG},
                { null, "test", "foo", null, "test", "foo", "test.foo", 0L, null, null, null, null, LONG },
                { null, "test", "parent", null, "test", "parent", "test.parent", 0L, null, null, null, null, LONG},
                { null, "test", "seq-table", null, "test", "seq-table", "test.seq-table", 0L, null, null, null, null, LONG},
                { null, "zap", "pow",  null, "zap", "pow", "zap.pow", 0L, null, null, null, null, LONG },
                { null, "zzz", "zzz1", null, "zzz", "zzz1", "zzz.zzz1", 0L, null, null, null, null, LONG },
                { null, "zzz", "zzz1", null, "zzz", "zzz2", "zzz.zzz1/zzz.zzz2", 1L, "zzz2/zzz1", null, "zzz", "zzz1_pkey", LONG },
        };

        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.GROUPING_CONSTRAINTS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S grouping_constraints", 22, skipped);
    }

    @Test
    public void keyColumnUsageScan() {    
        final Object[][] expected = {
                {null, "gco", "a/r", null, "gco","a", "pid", 0L, 0L, LONG },
                {null, "gco", "a_pkey", null, "gco", "a", "id", 0L, null, LONG },
                {null, "gco", "b/m", null, "gco", "b", "pid", 0L, 0L, LONG },
                {null, "gco", "b_pkey", null, "gco", "b",  "id", 0L, null, LONG },
                {null, "gco", "m/r", null, "gco", "m", "pid", 0L, 0L, LONG },
                {null, "gco", "m_pkey", null, "gco", "m", "id", 0L, null, LONG },
                {null, "gco", "r_pkey", null, "gco", "r","id", 0L, null, LONG },
                {null, "gco", "w/a", null, "gco", "w",  "pid", 0L, 0L, LONG },
                {null, "gco",  "x/b", null, "gco", "x", "pid", 0L, 0L, LONG },
                {null, "test", "bar_pkey",   null, "test", "bar", "col", 0L, null, LONG },
                {null, "test", "bar2/bar",  null, "test", "bar2", "pid", 0L, 0L, LONG },
                {null, "test", "child_pkey", null, "test", "child", "col1", 0L, null, LONG },
                {null, "test", "fkey_parent", null, "test", "child", "col2", 0L, 0L, LONG},
                {null, "test", "parent_pkey", null, "test", "parent", "col1", 0L, null, LONG},
                {null, "test", "seq-table_pkey",   null, "test", "seq-table", "col", 0L, null, LONG}, 
                {null, "zap", "pow_ukey", null, "zap", "pow", "name", 0L, null, LONG },
                {null, "zap", "pow_ukey",null, "zap", "pow", "value", 1L, null, LONG },
                {null, "zzz", "zzz1_pkey",   null, "zzz", "zzz1", "id", 0L, null, LONG },
                {null, "zzz", "zzz2/zzz1", null, "zzz", "zzz2", "one_id", 0L, 0L, LONG },
                {null, "zzz",  "zzz2_pkey",  null, "zzz", "zzz2","id", 0L, null, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.KEY_COLUMN_USAGE).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S key_column_usage", 0, skipped);
    }

    @Test
    public void indexesScan() {
        final Object[][] expected = {
                { null, "gco", "a", "PRIMARY", null, "gco", "a_pkey", LONG, "gco.a.PRIMARY",VARCHAR, "PRIMARY", true, null, null, LONG },
                { null, "gco", "b", "PRIMARY", null, "gco", "b_pkey", LONG, "gco.b.PRIMARY",VARCHAR, "PRIMARY", true, null, null, LONG },
                { null, "gco", "m", "PRIMARY", null, "gco", "m_pkey", LONG, "gco.m.PRIMARY",VARCHAR, "PRIMARY", true, null, null, LONG },
                { null, "gco", "r", "PRIMARY", null, "gco", "r_pkey", LONG, "gco.r.PRIMARY",VARCHAR, "PRIMARY", true, null, null, LONG },
                { null, "test", "bar", "PRIMARY", null, "test", "bar_pkey", LONG, "test.bar.PRIMARY",VARCHAR, "PRIMARY", true, null, null, LONG },
                { null, "test", "bar2", "foo_name", null, null, null, LONG, "test.bar.foo_name",VARCHAR, "INDEX", false, "RIGHT", null, LONG },
                { null, "test", "child", "PRIMARY", null, "test", "child_pkey", LONG, "test.child.PRIMARY",VARCHAR, "PRIMARY", true, null, null, LONG},
                { null, "test", "child", "fkey_parent", null, null, null, LONG, "test.child.fkey_parent",VARCHAR, "INDEX", false, null, null, LONG},
                { null, "test", "parent", "PRIMARY", null, "test", "parent_pkey", LONG, "test.parent.PRIMARY",VARCHAR, "PRIMARY", true, null, null, LONG},
                { null, "test", "seq-table", "PRIMARY", null, "test", "seq-table_pkey", LONG, "test.seq-table.PRIMARY",VARCHAR, "PRIMARY", true, null, null, LONG},
                { null, "zap", "pow", "name_value", null, "zap", "pow_ukey", LONG, "zap.pow.name_value",VARCHAR, "UNIQUE", true, null, null, LONG },
                { null, "zzz", "zzz1", "PRIMARY", null, "zzz", "zzz1_pkey", LONG, "zzz.zzz1.PRIMARY",VARCHAR, "PRIMARY", true, null, null, LONG },
                { null, "zzz", "zzz2", "PRIMARY", null, "zzz", "zzz2_pkey", LONG, "zzz.zzz2.PRIMARY",VARCHAR, "PRIMARY", true, null, null, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.INDEXES).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S indexes", 0, skipped);
    }

    @Test
    public void indexColumnsScan() {
        final Object[][] expected = {
                { null, "gco", "a", "PRIMARY", null, "gco", "a", "id", 0L, true, LONG },
                { null, "gco", "b", "PRIMARY", null, "gco", "b", "id", 0L, true, LONG },
                { null, "gco", "m", "PRIMARY", null, "gco", "m", "id", 0L, true, LONG },
                { null, "gco", "r", "PRIMARY", null, "gco", "r", "id", 0L, true, LONG },
                { null, "test", "bar", "PRIMARY", null, "test", "bar", "col", 0L, true, LONG },
                { null, "test", "bar2", "foo_name", null, "test", "bar2", "foo", 0L, true, LONG },
                { null, "test", "bar2", "foo_name", null, "test", "bar", "name", 1L, true, LONG },
                { null, "test", "child", "PRIMARY", null, "test", "child", "col1", 0L, true, LONG},
                { null, "test", "child", "fkey_parent", null, "test", "child", "col2", 0L, true, LONG},
                { null, "test", "parent", "PRIMARY", null, "test", "parent", "col1", 0L, true, LONG},
                { null, "test", "seq-table", "PRIMARY", null, "test", "seq-table", "col", 0L, true, LONG}, 
                { null, "zap",  "pow", "name_value",  null, "zap", "pow", "name", 0L, true, LONG },
                { null, "zap",  "pow", "name_value", null, "zap", "pow", "value", 1L, true, LONG },
                { null, "zzz", "zzz1", "PRIMARY",  null, "zzz", "zzz1", "id", 0L, true, LONG },
                { null, "zzz", "zzz2", "PRIMARY",  null, "zzz", "zzz2", "id", 0L, true, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.INDEX_COLUMNS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skipped I_S index_columns", 0, skipped);
    }

    @Test
    public void sequencesScan() {
        final Object[][] expected = {
                {null, "test", "_col_sequence", "bigint",   1L, 0L, 1000L, 1L, false, "test._col_sequence", LONG},
                {null, "test", "sequence",  "bigint",   1L, 0L, 1000L, 1L, false, "test.sequence", LONG },
                {null, "test", "sequence1", "bigint", 1000L, 0L, 1000L, -1L,false, "test.sequence1", LONG},
        };
        GroupScan scan = getFactory (BasicInfoSchemaTablesServiceImpl.SEQUENCES).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S sequences", 0, skipped);
    }

    @Test
    public void viewsScan() {
        final Object[][] expected = {
                { null, "test", "voo", new Text("CREATE VIEW voo(c1,c2) AS SELECT c2,c1 FROM foo"), "NONE", false, "NO", "NO", "NO", "NO", LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.VIEWS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S views", 0, skipped);
    }

    @Test
    public void viewTableUsageScan() {
        final Object[][] expected = {
                { null, "test", "voo", null, "test", "foo", LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.VIEW_TABLE_USAGE).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S views", 0, skipped);
    }

    @Test
    public void viewColumnUsageScan() {
        final Object[][] expected = {
                { null, "test", "voo", null, "test", "foo", "c1", LONG },
                { null, "test", "voo", null, "test", "foo", "c2", LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.VIEW_COLUMN_USAGE).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S views", 0, skipped);
    }

    @Test
    public void routinesScan() {
        final Object[][] expected = {
            { null, "test", "proc1", null, "test", "proc1", "PROCEDURE", null, null, null, null, null, null, "EXTERNAL", 
                null, "com.foundationdb.procs.Proc1.call", "java", "JAVA", 
                "NO", null, null, null, "YES", 0L, null, null, null, null, false, null, null, null, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.ROUTINES).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S routines", 0, skipped);
    }

    @Test
    public void parametersScan() {
        final Object[][] expected = {
            { null, "test", "proc1", "n1", 1L, "BIGINT", null, null, null, null, "IN", "NO", null, LONG },
            { null, "test", "proc1", "s1", 2L, "VARCHAR", 16L, null, null, null, "IN", "NO", null, LONG },
            { null, "test", "proc1", "n2", 3L, "DECIMAL", null, 10L, 10L, 5L, "IN", "NO", null, LONG },
            { null, "test", "proc1", null, 4L, "VARCHAR", 100L, null, null, null, "OUT", "NO", null, LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.PARAMETERS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S parameters", 0, skipped);
    }

    @Test
    public void jarsScan() {
        final Object[][] expected = {
            { null, "test", "ajar", "https://example.com/procs/ajar.jar", LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.JARS).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S jars", 0, skipped);
    }

    @Test
    public void routineJarUsageScan() {
        final Object[][] expected = {
                { null, "test", "proc1", null, "test", "ajar", LONG },
        };
        GroupScan scan = getFactory(BasicInfoSchemaTablesServiceImpl.ROUTINE_JAR_USAGE).getGroupScan(adapter);
        int skipped = scanAndCompare(expected, scan);
        assertEquals("Skip I_S routines", 0, skipped);
    }


    private static class MockSchemaManager implements SchemaManager {
        final AkibanInformationSchema ais;
        final TypesRegistry typesRegistry;
        final TypesTranslator typesTranslator;
        final StorageFormatRegistry storageFormatRegistry = DummyStorageFormatRegistry.create();

        public MockSchemaManager(AkibanInformationSchema ais, TypesRegistry typesRegistry, TypesTranslator typesTranslator) {
            this.ais = ais;
            this.typesRegistry = typesRegistry;
            this.typesTranslator = typesTranslator;
        }

        @Override
        public AkibanInformationSchema getAis(Session session) {
            return ais;
        }

        @Override
        public TypesRegistry getTypesRegistry() {
            return typesRegistry;
        }

        @Override
        public TypesTranslator getTypesTranslator() {
            return typesTranslator;
        }

        @Override
        public StorageFormatRegistry getStorageFormatRegistry() {
            return storageFormatRegistry;
        }

        @Override
        public AISCloner getAISCloner() {
            return new AISCloner(typesRegistry, storageFormatRegistry);
        }

        @Override
        public TableName registerStoredInformationSchemaTable(Table newTable, int version) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableName registerMemoryInformationSchemaTable(Table newTable, MemoryTableFactory factory) {
            Group group = newTable.getGroup();
            storageFormatRegistry.registerMemoryFactory(group.getName(), factory);
            // No copying or name registry; just apply right now.
            group.setStorageDescription(null);
            storageFormatRegistry.finishStorageDescription(group, null);
            return group.getName();
        }

        @Override
        public void unRegisterMemoryInformationSchemaTable(TableName tableName) {
            storageFormatRegistry.unregisterMemoryFactory(tableName);
        }

        @Override
        public void addOnlineHandledHKey(Session session, int tableID, Key hKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<byte[]> getOnlineHandledHKeyIterator(Session session, int tableID, Key hKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isOnlineActive(Session session, int tableID) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<OnlineChangeState> getOnlineChangeStates(Session session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void startOnline(Session session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setOnlineDMLError(Session session, int tableID, String message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getOnlineDMLError(Session session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AkibanInformationSchema getOnlineAIS(Session session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addOnlineChangeSet(Session session, ChangeSet changeSet) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<ChangeSet> getOnlineChangeSets(Session session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void finishOnline(Session session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void discardOnline(Session session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableName createTableDefinition(Session session, Table newTable) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameTable(Session session, TableName currentName, TableName newName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createIndexes(Session session, Collection<? extends Index> indexes, boolean keepStorage) {
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
        public void dropSchema(Session session, String schemaName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropNonSystemSchemas(Session session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterTableDefinitions(Session session, Collection<ChangedTableDescription> alteredTables) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterSequence(Session session, TableName sequenceName, Sequence newDefinition) {
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
        public void createRoutine(Session session, Routine routine, boolean replaceExisting) {
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
        public void registerSystemSQLJJar(SQLJJar sqljJar) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void unRegisterSystemSQLJJar(TableName jarName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getTreeNames(Session session) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getOldestActiveAISGeneration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Long> getActiveAISGenerations() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasTableChanged(Session session, int tableID) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setSecurityService(SecurityService securityService) {
        }
    }
}
