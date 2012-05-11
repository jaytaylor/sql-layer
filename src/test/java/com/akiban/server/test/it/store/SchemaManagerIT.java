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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.TableDefinition;
import com.akiban.server.test.it.ITBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.UserTable;
import com.akiban.server.service.config.Property;

public final class SchemaManagerIT extends ITBase {
    private final static String SCHEMA = "my_schema";
    private final static String VOL2_PREFIX = "foo_schema";
    private final static String VOL3_PREFIX = "bar_schema";

    private final static String T1_NAME = "t1";
    private final static String T1_DDL = "id int NOT NULL, PRIMARY KEY(id)";
    private final static String T2_NAME = "t2";
    private final static String T2_DDL = "id int NOT NULL, PRIMARY KEY(id)";
    private final static String T3_CHILD_T1_NAME = "t3";
    private final static String T3_CHILD_T1_DDL = "id int NOT NULL, t1id int, PRIMARY KEY(id), "+
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

    private void deleteTableDef(final String schema, final String table) throws Exception {
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
                schemaManager.deleteTableDefinition(session(), schema, table);
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

    private List<String> getSchemaStringsWithoutAIS(final boolean withGroupTables) throws Exception {
        return transactionally(new Callable<List<String>>() {
            public List<String> call() throws Exception {
                List<String> statements = schemaManager.schemaStrings(session(), withGroupTables);
                Iterator<String> it = statements.iterator();
                while(it.hasNext()) {
                    String ddl = it.next();
                    if(ddl.contains("akiban_information_schema")) {
                        it.remove();
                    }
                }
                return statements;
            }
        });
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
        final SortedMap<String, TableDefinition> defs = schemaManager.getTableDefinitions(session(), "fooschema");
        assertEquals("no defs", 0, defs.size());
    }

    @Test
    public void getTableDefinitionsOneSchema() throws Exception {
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        createTableDef(SCHEMA, T2_NAME, T2_DDL);
        createTableDef(SCHEMA + "_bob", T1_NAME, T1_DDL);
        final SortedMap<String, TableDefinition> defs = schemaManager.getTableDefinitions(session(), SCHEMA);
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

    @Test
    public void deleteUnknownDefinition() throws Exception {
        deleteTableDef("schema1", "table1");
        assertTablesInSchema(SCHEMA);
        deleteTableDef("schema1", "table1");
        assertTablesInSchema(SCHEMA);
    }

    @Test
    public void deleteDefinitionTwice() throws Exception {
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        assertTablesInSchema(SCHEMA, T1_NAME);
        
        deleteTableDef(SCHEMA, T1_NAME);
        assertTablesInSchema(SCHEMA);
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
        final long first = schemaManager.getUpdateTimestamp();
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        final long second = schemaManager.getUpdateTimestamp();
        assertTrue("timestamp changed", first != second);
    }

    @Test
    public void updateTimestampChangesWithDelete() throws Exception {
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        final long first = schemaManager.getUpdateTimestamp();
        deleteTableDef(SCHEMA, T1_NAME);
        final long second = schemaManager.getUpdateTimestamp();
        assertTrue("timestamp changed", first != second);
    }

    @Test
    public void schemaGenChangesWithCreate() throws Exception {
        final int first = schemaManager.getSchemaGeneration();
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        final int second = schemaManager.getSchemaGeneration();
        assertTrue("timestamp changed", first != second);
    }

    @Test
    public void schemaGenChangesWithDelete() throws Exception {
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        final int first = schemaManager.getSchemaGeneration();
        deleteTableDef(SCHEMA, T1_NAME);
        final int second = schemaManager.getSchemaGeneration();
        assertTrue("timestamp changed", first != second);
    }

    @Test
    public void forceNewTimestampChangesTimestamp() throws Exception {
        final long first = schemaManager.getUpdateTimestamp();
        schemaManager.forceNewTimestamp();
        final long second = schemaManager.getUpdateTimestamp();
        assertTrue("timestamp changed", first != second);
    }

    @Test
    public void forceNewTimestampChangesSchemaGen() throws Exception {
        final int first = schemaManager.getSchemaGeneration();
        schemaManager.forceNewTimestamp();
        final int second = schemaManager.getSchemaGeneration();
        assertTrue("timestamp changed", first != second);
    }

    @Test
    public void tableIDsAreLow() throws Exception {
        // TODO: Delete this test, only confirming temporarily desired behavior
        // Purely testing initial table IDs start at 1 and don't change when adding new tables
        // Partly required by com.akiban.qp.operator.AcenstorLookup_Default creating an array sized by max table id
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        assertTablesInSchema(SCHEMA, T1_NAME);
        assertEquals("t1 id", 1, getUserTable(SCHEMA, T1_NAME).getTableId().intValue());
        createTableDef(SCHEMA, T2_NAME, T2_DDL);
        assertTablesInSchema(SCHEMA, T1_NAME, T2_NAME);
        assertEquals("t1 id", 1, getUserTable(SCHEMA, T1_NAME).getTableId().intValue());
        // 3: t1 group table got 2
        assertEquals("t2 id", 3, getUserTable(SCHEMA, T2_NAME).getTableId().intValue());
        createTableDef(SCHEMA, T3_CHILD_T1_NAME, T3_CHILD_T1_DDL);
        assertTablesInSchema(SCHEMA, T1_NAME, T2_NAME, T3_CHILD_T1_NAME);
        assertEquals("t1 id", 1, getUserTable(SCHEMA, T1_NAME).getTableId().intValue());
        assertEquals("t2 id", 3, getUserTable(SCHEMA, T2_NAME).getTableId().intValue());
        // 4: t2 group table got 4
        assertEquals("t3 id", 5, getUserTable(SCHEMA, T3_CHILD_T1_NAME).getTableId().intValue());
    }

    @Test
    public void schemaStringsSingleTable() throws Exception {
        // Check 1) basic ordering 2) that the statements are 'canonicalized'
        final String TABLE_DDL =  "id int not null primary key";
        final String SCHEMA_DDL = "create schema if not exists `foo`;";
        final String TABLE_CANONICAL = "create table `foo`.`bar`(`id` int NOT NULL, PRIMARY KEY(`id`)) engine=akibandb";
        createTableDef("foo", "bar", TABLE_DDL);
        final List<String> ddls = getSchemaStringsWithoutAIS(false);
        assertEquals("ddl count", 2, ddls.size()); // schema and table
        assertTrue("create schema", ddls.get(0).startsWith("create schema"));
        assertEquals("create schema is canonical", SCHEMA_DDL, ddls.get(0));
        assertTrue("create table second", ddls.get(1).startsWith("create table"));
        assertEquals("create table is canonical", TABLE_CANONICAL, ddls.get(1));
    }

    @Test
    public void schemaStringsSingleGroup() throws Exception {
        createTableDef(SCHEMA, T1_NAME, T1_DDL);
        createTableDef(SCHEMA, T3_CHILD_T1_NAME, T3_CHILD_T1_DDL);
        Map<String, List<String>> schemaAndTables = new HashMap<String, List<String>>();
        schemaAndTables.put(SCHEMA, Arrays.asList(T1_NAME, T3_CHILD_T1_NAME));
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
        final int UT_COUNT = schemaManager.getAis(session()).getUserTables().size();
        final int GT_COUNT = schemaManager.getAis(session()).getGroupTables().size();

        String tableNames[] = new String[TABLE_COUNT];
        for(int i = 0; i < TABLE_COUNT; ++i) {
            tableNames[i] = "t" + i;
            createTable(SCHEMA, tableNames[i], "id int not null primary key");
        }

        AkibanInformationSchema ais = schemaManager.getAis(session());
        assertEquals("user tables count before", TABLE_COUNT + UT_COUNT, ais.getUserTables().size());
        assertEquals("group tables count before", TABLE_COUNT + GT_COUNT, ais.getGroupTables().size());
        assertTablesInSchema(SCHEMA, tableNames);

        safeRestartTestServices();

        schemaManager = serviceManager().getSchemaManager();
        ais = schemaManager.getAis(session());
        assertNotNull(ais);
        assertEquals("user tables count after", TABLE_COUNT + UT_COUNT, ais.getUserTables().size());
        assertEquals("group tables count after", TABLE_COUNT + GT_COUNT, ais.getGroupTables().size());
        assertTablesInSchema(SCHEMA, tableNames);
    }

    @Test
    public void multipleSchemasAndRestart() throws Exception {
        final int TABLE_COUNT = 3;
        AkibanInformationSchema ais = schemaManager.getAis(session());
        final int UT_COUNT = ais.getUserTables().size();
        final int GT_COUNT = ais.getGroupTables().size();

        createTable(SCHEMA+"1", "t1", "id int not null primary key");
        createTable(SCHEMA+"2", "t2", "id int not null primary key");
        createTable(SCHEMA+"3", "t3", "id int not null primary key");

        ais = schemaManager.getAis(session());
        assertEquals("user tables count", TABLE_COUNT + UT_COUNT, ais.getUserTables().size());
        assertEquals("group tables count", TABLE_COUNT + GT_COUNT, ais.getGroupTables().size());
        assertTablesInSchema(SCHEMA+"1", "t1");
        assertTablesInSchema(SCHEMA+"2", "t2");
        assertTablesInSchema(SCHEMA+"3", "t3");

        safeRestartTestServices();

        schemaManager = serviceManager().getSchemaManager();
        ais = schemaManager.getAis(session());
        assertNotNull("ais exists", ais);
        assertEquals("user tables count", TABLE_COUNT + UT_COUNT, ais.getUserTables().size());
        assertEquals("group tables count", TABLE_COUNT + GT_COUNT, ais.getGroupTables().size());
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
                {new TableName("foo$$_akiban_bar", "baz"), new TableName("foo", "bar$$_akiban_baz")}
        };

        for(TableName pair[] : testNames) {
            createTable(pair[0].getSchemaName(), pair[0].getTableName(), "id int not null primary key");
            createTable(pair[1].getSchemaName(), pair[1].getTableName(), "id int not null primary key");
            String treeName1 = ddl().getAIS(session()).getUserTable(pair[0]).getTreeName();
            String treeName2 = ddl().getAIS(session()).getUserTable(pair[1]).getTreeName();
            assertFalse("Non unique tree name: " + treeName1, treeName1.equals(treeName2));
        }
    }

    @Test
    public void changeInAISTableIsUpgradeIssue() throws Exception {
        /*
         * Simple sanity check. Change as needed but heed the
         * warnings that are in PSSM#createPrimordialAIS().
         */
        final String SCHEMA = "akiban_information_schema";
        final String STATS_TABLE = "zindex_statistics";
        final String ENTRY_TABLE = "zindex_statistics_entry";
        final String STATS_DDL = "create table `akiban_information_schema`.`zindex_statistics`("+
            "`table_id` int NOT NULL, `index_id` int NOT NULL, `analysis_timestamp` timestamp, "+
            "`row_count` bigint, `sampled_count` bigint, "+
            "PRIMARY KEY(`table_id`, `index_id`)"+
        ") engine=akibandb";
        final String ENTRY_DDL = "create table `akiban_information_schema`.`zindex_statistics_entry`("+
            "`table_id` int NOT NULL, `index_id` int NOT NULL, `column_count` int NOT NULL, "+
            "`item_number` int NOT NULL, `key_string` varchar(2048), `key_bytes` varbinary(4096), "+
            "`eq_count` bigint, `lt_count` bigint, `distinct_count` bigint, "+
            "PRIMARY KEY(`table_id`, `index_id`, `column_count`, `item_number`), "+
            "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0`(`table_id`, `index_id`) "+
                "REFERENCES `zindex_statistics`(`table_id`, `index_id`)"+
        ") engine=akibandb";

        TableDefinition statsDef = getTableDef(SCHEMA, STATS_TABLE);
        assertNotNull("Stats table present", statsDef);
        assertEquals("Stats DDL", STATS_DDL, statsDef.getDDL());

        TableDefinition entryDef = getTableDef(SCHEMA, ENTRY_TABLE);
        assertNotNull("Entry table present", entryDef);
        assertEquals("Entry DDL", ENTRY_DDL, entryDef.getDDL());
    }

    /**
     * Assert that the given tables in the given schema has the, and only the, given tables. Also
     * confirm each table exists in the AIS and has a definition.
     * @param schema Name of schema to check.
     * @param tableNames List of table names to check.
     * @throws Exception For any internal error.
     */
    private void assertTablesInSchema(String schema, String... tableNames) throws Exception {
        final SortedSet<String> expected = new TreeSet<String>();
        final AkibanInformationSchema ais = ddl().getAIS(session());
        final Session session = session();
        for (String name : tableNames) {
            final Table table = ais.getTable(schema, name);
            assertNotNull(schema + "." + name + " in AIS", table);
            final TableDefinition def = schemaManager.getTableDefinition(session, table.getName());
            assertNotNull(schema + "." + name  + " has definition", def);
            expected.add(name);
        }
        final SortedSet<String> actual = new TreeSet<String>();
        for (TableDefinition def : schemaManager.getTableDefinitions(session, schema).values()) {
            final Table table = ais.getTable(schema, def.getTableName());
            assertNotNull(def + " in AIS", table);
            actual.add(def.getTableName());
        }
        assertEquals("tables in: " + schema, expected, actual);
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
        final List<String> ddls = getSchemaStringsWithoutAIS(false);
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
}
