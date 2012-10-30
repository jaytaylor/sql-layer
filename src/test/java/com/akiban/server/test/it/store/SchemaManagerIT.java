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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.memoryadapter.MemoryAdapter;
import com.akiban.qp.memoryadapter.MemoryGroupCursor;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.IndexScanSelector;
import com.akiban.qp.memoryadapter.MemoryTableFactory;
import com.akiban.server.error.DuplicateTableNameException;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.ISTableVersionMismatchException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.JoinToProtectedTableException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.TableDefinition;
import com.akiban.server.store.statistics.IndexStatistics;
import com.akiban.server.test.it.ITBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.UserTable;
import com.akiban.server.service.config.Property;

public final class SchemaManagerIT extends ITBase {
    final static String SCHEMA = "my_schema";
    final static String VOL2_PREFIX = "foo_schema";
    final static String VOL3_PREFIX = "bar_schema";

    final static String T1_NAME = "t1";
    final static String T1_DDL = "id int NOT NULL, PRIMARY KEY(id)";
    final static String T2_NAME = "t2";
    final static String T2_DDL = "id int NOT NULL, PRIMARY KEY(id)";
    final static String T3_CHILD_T1_NAME = "t3";
    final static String T3_CHILD_T1_DDL = "id int NOT NULL, t1id int, PRIMARY KEY(id), "+
                                          "grouping foreign key(t1id) references t1(id)";

    private SchemaManager schemaManager;

    private void createTableDef(final String schema, final String tableName, final String ddl) throws Exception {
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
                createTable(schema, tableName, ddl);
                return null;
            }
        });
    }

    private void registerISTable(final UserTable table, final MemoryTableFactory factory) throws Exception {
        schemaManager.registerMemoryInformationSchemaTable(table, factory);
    }

    private void registerISTable(final UserTable table, final int version) throws Exception {
        schemaManager.registerStoredInformationSchemaTable(table, version);
    }

    private void deleteTableDef(final String schema, final String table) throws Exception {
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
                schemaManager.dropTableDefinition(session(), schema, table, SchemaManager.DropBehavior.RESTRICT);
                return null;
            }
        });
    }

    private TableDefinition getTableDef(final String schema, final String table) throws Exception {
        return transactionally(new Callable<TableDefinition>() {
            public TableDefinition call() throws Exception {
                return schemaManager.getTableDefinition(session(), new TableName(schema, table));
            }
        });
    }

    private List<String> getSchemaStringsWithoutAIS() throws Exception {
        return transactionally(new Callable<List<String>>() {
            public List<String> call() throws Exception {
                return schemaManager.schemaStrings(session(), false);
            }
        });
    }

    private void safeRestart() throws Exception {
        safeRestartTestServices();
        schemaManager = serviceManager().getSchemaManager();
    }
    
    @Override
    protected Collection<Property> startupConfigProperties() {
        // Set up multi-volume treespace policy so we can be sure schema is properly distributed.
        final Collection<Property> properties = new ArrayList<Property>();
        properties.add(new Property("akserver.treespace.a",
                                    VOL2_PREFIX + "*:${datapath}/${schema}.v0,create,pageSize:${buffersize},"
                                    + "initialSize:10K,extensionSize:1K,maximumSize:10G"));
        properties.add(new Property("akserver.treespace.b",
                                    VOL3_PREFIX + "*:${datapath}/${schema}.v0,create,pageSize:${buffersize},"
                                    + "initialSize:10K,extensionSize:1K,maximumSize:10G"));
        return properties;
    }

    @Before
    public void setUp() throws Exception {
        schemaManager = serviceManager().getSchemaManager();
        assertTablesInSchema(SCHEMA);
    }

    @Test(expected=NoSuchTableException.class)
    public void getUnknownTableDefinition() throws Exception {
        getTableDef("fooschema", "bartable1");
    }

    // Also tests createDef(), but assertTablesInSchema() uses getDef() so try and test first
    @Test
    public void getTableDefinition() throws Exception {
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        final TableDefinition def = getTableDef(SCHEMA, T1_NAME);
        assertNotNull("Definition exists", def);
    }

    @Test
    public void getTableDefinitionsUnknownSchema() throws Exception {
        final SortedMap<String, TableDefinition> defs = getTableDefinitions("fooschema");
        assertEquals("no defs", 0, defs.size());
    }

    @Test
    public void getTableDefinitionsOneSchema() throws Exception {
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        createTableDef(SCHEMA, T2_NAME, T2_DDL);
        createTableDef(SCHEMA + "_bob", T1_NAME, T1_DDL);
        final SortedMap<String, TableDefinition> defs = getTableDefinitions(SCHEMA);
        assertTrue("contains t1", defs.containsKey(T1_NAME));
        assertTrue("contains t2", defs.containsKey(T2_NAME));
        assertEquals("no defs", 2, defs.size());
    }

    @Test
    public void createOneDefinition() throws Exception {
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        assertTablesInSchema(SCHEMA, T1_NAME);
    }

    @Test
    public void deleteOneDefinition() throws Exception {
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        assertTablesInSchema(SCHEMA, T1_NAME);
        deleteTableDef(SCHEMA, T1_NAME);
        assertTablesInSchema(SCHEMA);
    }

    @Test(expected=NoSuchTableException.class)
    public void deleteUnknownDefinition() throws Exception {
        assertTablesInSchema(SCHEMA);
        deleteTableDef("schema1", "table1");
    }

    @Test
    public void deleteDefinitionTwice() throws Exception {
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        assertTablesInSchema(SCHEMA, T1_NAME);
        
        deleteTableDef(SCHEMA, T1_NAME);
        assertTablesInSchema(SCHEMA);
    }

    @Test
    public void deleteTwoDefinitions() throws Exception {
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        assertTablesInSchema(SCHEMA, T1_NAME);

        createTableDef(SCHEMA, T2_NAME, T2_DDL);
        assertTablesInSchema(SCHEMA, T1_NAME, T2_NAME);

        deleteTableDef(SCHEMA, T1_NAME);
        assertTablesInSchema(SCHEMA, T2_NAME);

        deleteTableDef(SCHEMA, T2_NAME);
        assertTablesInSchema(SCHEMA);
    }

    @Test
    public void deleteChildDefinition() throws Exception {
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        assertTablesInSchema(SCHEMA, T1_NAME);

        createTableDef(SCHEMA, T3_CHILD_T1_NAME, T3_CHILD_T1_DDL);
        assertTablesInSchema(SCHEMA, T1_NAME, T3_CHILD_T1_NAME);

        // Deleting child should not delete parent
        deleteTableDef(SCHEMA, T3_CHILD_T1_NAME);
        assertTablesInSchema(SCHEMA, T1_NAME);

        deleteTableDef(SCHEMA, T1_NAME);
        assertTablesInSchema(SCHEMA);
    }

    @Test
    public void deleteParentDefinitionFirst() throws Exception {
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        assertTablesInSchema(SCHEMA, T1_NAME);

        createTableDef(SCHEMA, T3_CHILD_T1_NAME, T3_CHILD_T1_DDL);
        assertTablesInSchema(SCHEMA, T1_NAME, T3_CHILD_T1_NAME);

        final AkibanInformationSchema ais = ddl().getAIS(session());
        final UserTable t1 = ais.getUserTable(SCHEMA, T1_NAME);
        assertNotNull("t1 exists", t1);
        final UserTable t3 = ais.getUserTable(SCHEMA, T3_CHILD_T1_NAME);
        assertNotNull("t3 exists", t3);

        // Double check grouping we are expecting
        assertNotNull("t3 has parent", t3.getParentJoin());
        assertSame("t1 is t3 parent", t1, t3.getParentJoin().getParent());
        assertNotNull("t1 has children", t1.getCandidateChildJoins());
        assertEquals("t1 has 1 child", 1, t1.getCandidateChildJoins().size());
        assertSame("t3 is t1 child", t3, t1.getCandidateChildJoins().get(0).getChild());
        
        try {
            deleteTableDef(SCHEMA, T1_NAME);
            Assert.fail("Exception expected!");
        } catch(InvalidOperationException e) {
            assertEquals("error code", ErrorCode.REFERENCED_TABLE, e.getCode());
        }

        assertTablesInSchema(SCHEMA, T1_NAME, T3_CHILD_T1_NAME);
        deleteTableDef(SCHEMA, T3_CHILD_T1_NAME);
        assertTablesInSchema(SCHEMA, T1_NAME);
        deleteTableDef(SCHEMA, T1_NAME);
        assertTablesInSchema(SCHEMA);
    }

    @Test
    public void createTwoDefinitionsTwoVolumes() throws Exception {
        final String SCHEMA_VOL2_A = VOL2_PREFIX + "_a";
        final String SCHEMA_VOL2_B = VOL2_PREFIX + "_b";

        assertTablesInSchema(SCHEMA_VOL2_A);
        assertTablesInSchema(SCHEMA_VOL2_B);
        assertTablesInSchema(SCHEMA);

        createTableDef(SCHEMA_VOL2_A, T1_NAME, T1_DDL);
        assertTablesInSchema(SCHEMA_VOL2_A, T1_NAME);
        assertTablesInSchema(SCHEMA);

        createTableDef(SCHEMA_VOL2_B, T2_NAME, T2_DDL);
        assertTablesInSchema(SCHEMA_VOL2_B, T2_NAME);
        assertTablesInSchema(SCHEMA_VOL2_A, T1_NAME);
        assertTablesInSchema(SCHEMA);
    }

    @Test
    public void deleteTwoDefinitionsTwoVolumes() throws Exception {
        final String SCHEMA_VOL2_A = VOL2_PREFIX + "_a";
        final String SCHEMA_VOL2_B = VOL2_PREFIX + "_b";

        createTableDef(SCHEMA_VOL2_A, T1_NAME, T1_DDL);
        createTableDef(SCHEMA_VOL2_B, T2_NAME, T2_DDL);
        assertTablesInSchema(SCHEMA_VOL2_A, T1_NAME);
        assertTablesInSchema(SCHEMA_VOL2_B, T2_NAME);

        deleteTableDef(SCHEMA_VOL2_A, T1_NAME);
        assertTablesInSchema(SCHEMA_VOL2_A);
        assertTablesInSchema(SCHEMA_VOL2_B, T2_NAME);

        deleteTableDef(SCHEMA_VOL2_B, T2_NAME);
        assertTablesInSchema(SCHEMA_VOL2_A);
        assertTablesInSchema(SCHEMA_VOL2_B);
    }


    @Test
    public void updateTimestampChangesWithCreate() throws Exception {
        final long first = ais().getGeneration();
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        final long second = ais().getGeneration();
        assertTrue("timestamp changed", first != second);
    }

    @Test
    public void updateTimestampChangesWithDelete() throws Exception {
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        final long first = ais().getGeneration();
        deleteTableDef(SCHEMA, T1_NAME);
        final long second = ais().getGeneration();
        assertTrue("timestamp changed", first != second);
    }

    @Test
    public void tableIDsAreLow() throws Exception {
        // Test confirming desired behavior, but edit as needed
        // Purely testing initial table IDs start at 1 and don't change when adding new tables
        // Partly required by com.akiban.qp.operator.AcenstorLookup_Default creating an array sized by max table id
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        assertTablesInSchema(SCHEMA, T1_NAME);
        assertEquals("t1 id", 1, getUserTable(SCHEMA, T1_NAME).getTableId().intValue());
        createTableDef(SCHEMA, T2_NAME, T2_DDL);
        assertTablesInSchema(SCHEMA, T1_NAME, T2_NAME);
        assertEquals("t1 id", 1, getUserTable(SCHEMA, T1_NAME).getTableId().intValue());
        assertEquals("t2 id", 2, getUserTable(SCHEMA, T2_NAME).getTableId().intValue());
        createTableDef(SCHEMA, T3_CHILD_T1_NAME, T3_CHILD_T1_DDL);
        assertTablesInSchema(SCHEMA, T1_NAME, T2_NAME, T3_CHILD_T1_NAME);
        assertEquals("t1 id", 1, getUserTable(SCHEMA, T1_NAME).getTableId().intValue());
        assertEquals("t2 id", 2, getUserTable(SCHEMA, T2_NAME).getTableId().intValue());
        assertEquals("t3 id", 3, getUserTable(SCHEMA, T3_CHILD_T1_NAME).getTableId().intValue());
    }

    @Test
    public void schemaStringsSingleTable() throws Exception {
        // Check 1) basic ordering 2) that the statements are 'canonicalized'
        final String TABLE_DDL =  "id int not null primary key";
        final String SCHEMA_DDL = "create schema if not exists `foo`;";
        final String TABLE_CANONICAL = "create table `foo`.`bar`(`id` int NOT NULL, PRIMARY KEY(`id`)) engine=akibandb DEFAULT CHARSET=utf8 COLLATE=utf8_bin";
        createTableDef("foo", "bar", TABLE_DDL);
        final List<String> ddls = getSchemaStringsWithoutAIS();
        assertEquals("ddl count", 2, ddls.size()); // schema and table
        assertTrue("create schema", ddls.get(0).startsWith("create schema"));
        assertEquals("create schema is canonical", SCHEMA_DDL, ddls.get(0));
        assertTrue("create table second", ddls.get(1).startsWith("create table"));
        assertEquals("create table is canonical", TABLE_CANONICAL, ddls.get(1));
    }

    @Test
    public void schemaStringsSingleGroup() throws Exception {
        final String SCHEMA = "s1";
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        createTableDef(SCHEMA, T3_CHILD_T1_NAME, T3_CHILD_T1_DDL);
        Map<String, List<String>> schemaAndTables = new HashMap<String, List<String>>();
        schemaAndTables.put(SCHEMA, Arrays.asList(T1_NAME, T3_CHILD_T1_NAME));
        assertSchemaStrings(schemaAndTables);
    }

    @Test
    public void schemaStringsMultipleSchemas() throws Exception {
        final Map<String, List<String>> schemaAndTables = new HashMap<String, List<String>>();
        schemaAndTables.put("s1", Arrays.asList("t1", "t2"));
        schemaAndTables.put("s2", Arrays.asList("t3"));
        schemaAndTables.put("s3", Arrays.asList("t4"));
        for(Map.Entry<String, List<String>> entry : schemaAndTables.entrySet()) {
            for(String table : entry.getValue()) {
                createTableDef(entry.getKey(), table, "id int not null primary key");
            }
        }
        assertSchemaStrings(schemaAndTables);
    }

    @Test
    public void manyTablesAndRestart() throws Exception {
        final int TABLE_COUNT = 50;
        final int UT_COUNT = ais().getUserTables().size();

        String tableNames[] = new String[TABLE_COUNT];
        for(int i = 0; i < TABLE_COUNT; ++i) {
            tableNames[i] = "t" + i;
            createTable(SCHEMA, tableNames[i], "id int not null primary key");
        }

        AkibanInformationSchema ais = ais();
        Collection<UserTable> before = new ArrayList<UserTable>(ais.getUserTables().values());
        assertEquals("user tables count before", TABLE_COUNT + UT_COUNT, ais.getUserTables().size());
        assertTablesInSchema(SCHEMA, tableNames);

        safeRestart();
        ais = ais();
        assertNotNull(ais);
        Collection<UserTable> after = ais.getUserTables().values();
        // Diagnostics for occasional assertion violation of user table count
        if (ais.getUserTables().size() != TABLE_COUNT + UT_COUNT) {
            System.out.println("BEFORE");
            for (UserTable userTable : before) {
                System.out.println(String.format("    %s", userTable));
            }
            System.out.println("AFTER");
            for (UserTable userTable : after) {
                System.out.println(String.format("    %s", userTable));
            }
        }
        assertEquals("user tables count after", TABLE_COUNT + UT_COUNT, ais.getUserTables().size());
        assertTablesInSchema(SCHEMA, tableNames);
    }

    @Test
    public void multipleSchemasAndRestart() throws Exception {
        final int TABLE_COUNT = 3;
        AkibanInformationSchema ais = ais();
        final int UT_COUNT = ais.getUserTables().size();

        createTable(SCHEMA+"1", "t1", "id int not null primary key");
        createTable(SCHEMA+"2", "t2", "id int not null primary key");
        createTable(SCHEMA+"3", "t3", "id int not null primary key");

        ais = ais();
        assertEquals("user tables count", TABLE_COUNT + UT_COUNT, ais.getUserTables().size());
        assertTablesInSchema(SCHEMA+"1", "t1");
        assertTablesInSchema(SCHEMA+"2", "t2");
        assertTablesInSchema(SCHEMA+"3", "t3");

        safeRestart();
        ais = ais();
        assertNotNull("ais exists", ais);

        assertEquals("user tables count", TABLE_COUNT + UT_COUNT, ais.getUserTables().size());
        assertTablesInSchema(SCHEMA+"1", "t1");
        assertTablesInSchema(SCHEMA+"2", "t2");
        assertTablesInSchema(SCHEMA+"3", "t3");
    }

    @Test
    public void treeNamesAreUnique() {
        TableName testNames[][] = {
                // These broke simple concat(s,'.',t) that was in RowDefCache
                {new TableName("foo.bar", "baz"), new TableName("foo", "bar.baz")},
                // These broke actual tree name generation
                {new TableName("foo$$_akiban_bar", "baz"), new TableName("foo", "bar$$_akiban_baz")},
                // New tree name separator
                {new TableName("tes.", "tt1"), new TableName("tes", ".tt1")}
        };

        for(TableName pair[] : testNames) {
            createTable(pair[0].getSchemaName(), pair[0].getTableName(), "id int not null primary key");
            createTable(pair[1].getSchemaName(), pair[1].getTableName(), "id int not null primary key");
            String treeName1 = ddl().getAIS(session()).getUserTable(pair[0]).getGroup().getTreeName();
            String treeName2 = ddl().getAIS(session()).getUserTable(pair[1]).getGroup().getTreeName();
            assertFalse("Non unique tree name: " + treeName1, treeName1.equals(treeName2));
        }
    }

    @Test
    public void crossSchemaGroups() throws Exception {
        final String SCHEMA1 = "schema1";
        final String SCHEMA2 = "schema2";
        final TableName PARENT1 = new TableName(SCHEMA1, "parent1");
        final TableName CHILD1 = new TableName(SCHEMA2, "child1");
        final TableName PARENT2 = new TableName(SCHEMA2, "parent2");
        final TableName CHILD2 = new TableName(SCHEMA1, "child2");
        final String T2_CHILD_DDL = T2_DDL + ", t1id int, grouping foreign key(t1id) references %s";

        // parent in schema1, child in schema2
        createTable(PARENT1.getSchemaName(), PARENT1.getTableName(), T1_DDL);
        createTable(CHILD1.getSchemaName(), CHILD1.getTableName(), String.format(T2_CHILD_DDL, PARENT1));
        // child in schema1, child in schema2
        createTable(PARENT2.getSchemaName(), PARENT2.getTableName(), T1_DDL);
        createTable(CHILD2.getSchemaName(), CHILD2.getTableName(), String.format(T2_CHILD_DDL, PARENT2));

        safeRestart();

        assertTablesInSchema(SCHEMA1, PARENT1.getTableName(), CHILD2.getTableName());
        assertTablesInSchema(SCHEMA2, PARENT2.getTableName(), CHILD1.getTableName());
        UserTable parent1 = ddl().getUserTable(session(), PARENT1);
        assertEquals("parent1 and child1 group", parent1.getGroup(), ddl().getUserTable(session(), CHILD1).getGroup());
        UserTable parent2 = ddl().getUserTable(session(), PARENT2);
        assertEquals("parent2 and child2 group", parent2.getGroup(), ddl().getUserTable(session(), CHILD2).getGroup());
    }

    @Test
    public void changeInAISTableIsUpgradeIssue() throws Exception {
        /*
         * Simple sanity check. Change as needed but remember it is an UPGRADE ISSUE.
         */
        final String SCHEMA = "information_schema";
        final String STATS_TABLE = "index_statistics";
        final String ENTRY_TABLE = "index_statistics_entry";
        final String STATS_DDL = "create table `information_schema`.`index_statistics`("+
            "`table_id` int NOT NULL, `index_id` int NOT NULL, `analysis_timestamp` timestamp, "+
            "`row_count` bigint, `sampled_count` bigint, "+
            "PRIMARY KEY(`table_id`, `index_id`)"+
        ") engine=akibandb DEFAULT CHARSET=utf8 COLLATE=utf8_bin";
        final String ENTRY_DDL = "create table `information_schema`.`index_statistics_entry`("+
            "`table_id` int NOT NULL, `index_id` int NOT NULL, `column_count` int NOT NULL, "+
            "`item_number` int NOT NULL, `key_string` varchar(2048) CHARACTER SET latin1, `key_bytes` varbinary(4096), "+
            "`eq_count` bigint, `lt_count` bigint, `distinct_count` bigint, "+
            "PRIMARY KEY(`table_id`, `index_id`, `column_count`, `item_number`), "+
            "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0`(`table_id`, `index_id`) "+
                "REFERENCES `index_statistics`(`table_id`, `index_id`)"+
        ") engine=akibandb DEFAULT CHARSET=utf8 COLLATE=utf8_bin";

        TableDefinition statsDef = getTableDef(SCHEMA, STATS_TABLE);
        assertNotNull("Stats table present", statsDef);
        assertEquals("Stats DDL", STATS_DDL, statsDef.getDDL());

        TableDefinition entryDef = getTableDef(SCHEMA, ENTRY_TABLE);
        assertNotNull("Entry table present", entryDef);
        assertEquals("Entry DDL", ENTRY_DDL, entryDef.getDDL());
    }

    @Test
    public void renameAndRecreate() throws Exception {
        createTable(SCHEMA, T1_NAME, T1_DDL);
        ddl().renameTable(session(), tableName(SCHEMA, T1_NAME), tableName("foo", "bar"));
        createTable(SCHEMA, T1_NAME, T1_DDL);

        String originalTreeName = getUserTable(SCHEMA, T1_NAME).getGroup().getTreeName();
        String newTreeName = getUserTable("foo", "bar").getGroup().getTreeName();
        assertTrue("Unique tree names", !originalTreeName.equals(newTreeName));
    }

    @Test
    public void createRestartAndCreateMore() throws Exception {
        createTable(SCHEMA, T1_NAME, T1_DDL);
        createTable(SCHEMA, T3_CHILD_T1_NAME, T3_CHILD_T1_DDL);
        createTable(SCHEMA, T2_NAME, T2_DDL);
        assertTablesInSchema(SCHEMA, T1_NAME, T2_NAME, T3_CHILD_T1_NAME);
        safeRestart();
        assertTablesInSchema(SCHEMA, T1_NAME, T2_NAME, T3_CHILD_T1_NAME);
        createIndex(SCHEMA, T2_NAME, "id_2", "id");
    }

    @Test
    public void registerMemoryTableBasic() throws Exception {
        final TableName tableName = new TableName(TableName.INFORMATION_SCHEMA, "test_table");
        MemoryTableFactory factory = new MemoryTableFactoryMock();
        registerISTable(makeSimpleISTable(tableName), factory);

        {
            UserTable testTable = ddl().getAIS(session()).getUserTable(tableName);
            assertNotNull("New table exists", testTable);
            assertEquals("Is memoryTable", true, testTable.hasMemoryTableFactory());
            assertSame("Exact factory preserved", factory, testTable.getMemoryTableFactory());
        }

        createTable(SCHEMA, T1_NAME, T1_DDL);
        {
            UserTable testTable = ddl().getAIS(session()).getUserTable(tableName);
            assertNotNull("New table exists after DDL", testTable);
            assertEquals("Is memoryTable after more DDL", true, testTable.hasMemoryTableFactory());
            assertSame("Exact factory preserved after more DDL", factory, testTable.getMemoryTableFactory());
        }

        {
            safeRestart();
            UserTable testTable = ddl().getAIS(session()).getUserTable(tableName);
            assertNull("Table did not survive restart", testTable);
        }
    }

    @Test
    public void noDuplicateMemoryTables() throws Exception {
        final TableName tableName = new TableName(TableName.INFORMATION_SCHEMA, "test_table");
        final UserTable sourceTable = makeSimpleISTable(tableName);
        MemoryTableFactory factory = new MemoryTableFactoryMock();
        registerISTable(sourceTable, factory);
        try {
            registerISTable(sourceTable, factory);
            fail("Expected DuplicateTableNameException");
        } catch(DuplicateTableNameException e) {
            // expected
        } finally {
            schemaManager.unRegisterMemoryInformationSchemaTable(tableName);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void noNullMemoryTableFactory() throws Exception {
        final TableName tableName = new TableName(TableName.INFORMATION_SCHEMA, "test_table");
        registerISTable(makeSimpleISTable(tableName), null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void noMemoryTableOutsideAISSchema() throws Exception {
        final TableName tableName = new TableName("foo", "test_table");
        registerISTable(makeSimpleISTable(tableName), null);
    }

    @Test
    public void registerStoredTableBasic() throws Exception {
        final Integer VERSION = 5;
        final TableName tableName = new TableName(TableName.INFORMATION_SCHEMA, "test_table");

        registerISTable(makeSimpleISTable(tableName), VERSION);
        {
            UserTable testTable = ddl().getAIS(session()).getUserTable(tableName);
            assertNotNull("New table exists", testTable);
            assertEquals("Exact version is preserved", VERSION, testTable.getVersion());
        }

        createTable(SCHEMA, T1_NAME, T1_DDL);
        {
            UserTable testTable = ddl().getAIS(session()).getUserTable(tableName);
            assertNotNull("New table exists after DDL", testTable);
            assertEquals("Exact version preserved after more DDL", VERSION, testTable.getVersion());
        }

        {
            safeRestart();
            UserTable testTable = ddl().getAIS(session()).getUserTable(tableName);
            assertNotNull("Table survived restart", testTable);
            assertEquals("Exact version preserved after more DDL", VERSION, testTable.getVersion());
        }
    }

    @Test
    public void canRegisterStoredTableWithSameVersion() throws Exception {
        final Integer VERSION = 5;
        final TableName tableName = new TableName(TableName.INFORMATION_SCHEMA, "test_table");
        final UserTable sourceTable = makeSimpleISTable(tableName);
        registerISTable(sourceTable, VERSION);
        registerISTable(sourceTable, VERSION);
    }

    @Test(expected=ISTableVersionMismatchException.class)
    public void cannotRegisterStoredTableWithDifferentVersion() throws Exception {
        final Integer VERSION = 5;
        final TableName tableName = new TableName(TableName.INFORMATION_SCHEMA, "test_table");
        final UserTable sourceTable = makeSimpleISTable(tableName);
        registerISTable(sourceTable, VERSION);
        registerISTable(sourceTable, VERSION + 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void noStoredTableOutsideAISSchema() throws Exception {
        final int VERSION = 5;
        final TableName tableName = new TableName("foo", "test_table");
        registerISTable(makeSimpleISTable(tableName), VERSION);
    }

    @Test
    public void sameRootNameMultipleSchemasAndRestart() throws Exception {
        final String SCHEMA1 = SCHEMA + "1";
        final String SCHEMA2 = SCHEMA + "2";
        createTable(SCHEMA1, T1_NAME, T1_DDL);
        createTable(SCHEMA2, T1_NAME, T1_DDL);
        assertTablesInSchema(SCHEMA1, T1_NAME);
        assertTablesInSchema(SCHEMA2, T1_NAME);

        safeRestart();
        assertTablesInSchema(SCHEMA1, T1_NAME);
        assertTablesInSchema(SCHEMA2, T1_NAME);
    }

    @Test(expected=JoinToProtectedTableException.class)
    public void joinToISTable() throws Exception {
        TableName name = new TableName(TableName.INFORMATION_SCHEMA, "p");
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA);
        builder.userTable(name).colLong("id", false).pk("id");
        builder.userTable(T1_NAME).colLong("id", false).colLong("pid", true).pk("id").joinTo("information_schema", "p").on("pid", "id");
        registerISTable(builder.unvalidatedAIS().getUserTable(name), new MemoryTableFactoryMock());
        ddl().createTable(session(), builder.unvalidatedAIS().getUserTable(SCHEMA, T1_NAME));
    }

    /**
     * Check that the result of {@link SchemaManager#schemaStrings(Session, boolean)} is correct for
     * the given tables. The only guarantees are that schemas are created with 'if not exists',
     * a schema statement comes before any table in it, and a create table statement is fully qualified.
     * @param schemaAndTables Map of schema names to table names that should exist
     * @throws Exception For any internal error.
     */
    private void assertSchemaStrings(Map<String, List<String>> schemaAndTables) throws Exception {
        final String CREATE_SCHEMA = "create schema if not exists `";
        final String CREATE_TABLE = "create table `";
        final List<String> ddls = getSchemaStringsWithoutAIS();
        final Set<String> sawSchemas = new HashSet<String>();
        for(String statement : ddls) {
            if(statement.startsWith(CREATE_SCHEMA)) {
                final int offset = CREATE_SCHEMA.length();
                final String schemaName = statement.substring(offset, offset + 2);
                assertFalse("haven't seen schema "+schemaName,
                            sawSchemas.contains(schemaName));
                sawSchemas.add(schemaName);
            }
            else if(statement.startsWith(CREATE_TABLE)){
                final int offset = CREATE_TABLE.length();
                final String schemaName = statement.substring(offset, offset + 2);
                assertTrue("schema "+schemaName+" has been seen",
                           sawSchemas.contains(schemaName));
                final String tableName = statement.substring(offset+5, offset+7);
                assertTrue("table "+tableName+" is in schema "+tableName,
                           schemaAndTables.get(schemaName).contains(tableName));
            }
            else {
                Assert.fail("Unknown statement type: " + statement);
            }
        }
    }

    private SortedMap<String, TableDefinition> getTableDefinitions(final String schema) {
        return transactionallyUnchecked(new Callable<SortedMap<String, TableDefinition>>() {
            @Override
            public SortedMap<String, TableDefinition> call() throws Exception {
                return schemaManager.getTableDefinitions(session(), schema);
            }
        });
    }

    /**
     * Assert that the given tables in the given schema has the, and only the, given tables. Also
     * confirm each table exists in the AIS and has a definition.
     * @param schema Name of schema to check.
     * @param tableNames List of table names to check.
     * @throws Exception For any internal error.
     */
    private void assertTablesInSchema(String schema, String... tableNames) {
        final SortedSet<String> expected = new TreeSet<String>();
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : tableNames) {
            final Table table = ais.getTable(schema, name);
            assertNotNull(schema + "." + name + " in AIS", table);
            final TableDefinition def = getTableDefinitions(schema).get(table.getName().getTableName());
            assertNotNull(schema + "." + name  + " has definition", def);
            expected.add(name);
        }
        final SortedSet<String> actual = new TreeSet<String>();
        for (TableDefinition def : getTableDefinitions(schema).values()) {
            final Table table = ais.getTable(schema, def.getTableName());
            assertNotNull(def + " in AIS", table);
            actual.add(def.getTableName());
        }
        assertEquals("tables in: " + schema, expected, actual);
    }

    private static UserTable makeSimpleISTable(TableName name) {
        NewAISBuilder builder = AISBBasedBuilder.create(name.getSchemaName());
        builder.userTable(name.getTableName()).colLong("id", false).pk("id");
        return builder.ais().getUserTable(name);
    }

    private static class MemoryTableFactoryMock implements MemoryTableFactory {
        @Override
        public TableName getName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MemoryGroupCursor.GroupScan getGroupScan(MemoryAdapter adapter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cursor getIndexCursor(Index index, Session session, IndexKeyRange keyRange, API.Ordering ordering, IndexScanSelector scanSelector) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long rowCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexStatistics computeIndexStatistics(Session session, Index index) {
            throw new UnsupportedOperationException();
        }
    }
}
