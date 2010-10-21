package com.akiban.cserver.store;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.message.ErrorCode;
import com.akiban.util.Strings;

public final class PersistitStoreSchemaManagerTest {

    private final static String SCHEMA = "my_schema";
    private final static Pattern REGEX = Pattern.compile("CREATE TABLE `(\\w+)`\\.(\\w+)");

    private PersistitStore store;
    
    PersistitStoreSchemaManager manager;

    @Before
    public void setUp() throws Exception {
        store = ServiceManagerImpl.getStoreForUnitTests();
        manager = store.getSchemaManager();
    }

    @After
    public void tearDown() throws Exception {
        try {
            assertEquals("user tables in AIS", 0, manager.getAisCopy().getUserTables().size());
            assertTables("user tables");
            assertDDLS();
        } finally {
            store.stop();
        }
    }

    private void createTable(ErrorCode expectedCode, String schema, String ddl) throws Exception {
        ErrorCode actualCode  = null;
        try {
            manager.createTable(schema, ddl);
        }
        catch (InvalidOperationException e) {
            actualCode = e.getCode();
        }
        assertEquals("createTable return value", expectedCode, actualCode);
    }
    
    private void createTable(String schema, String ddl) throws Exception {
        manager.createTable(schema, ddl);
    }

    @Test
    public void testUtf8Table() throws Exception {
        createTable(ErrorCode.UNSUPPORTED_CHARSET, SCHEMA,
                "CREATE TABLE myvarchartest(id int key, name varchar(32) character set UTF8) engine = akibadb");
        createTable(ErrorCode.UNSUPPORTED_CHARSET, SCHEMA,
                "CREATE TABLE myvarchartest(id int key, name varchar(32) character set utf8) engine = akibadb");
    }

    @Test
    public void testAddDropOneTable() throws Exception {

        createTable(SCHEMA, "CREATE TABLE one (id int, PRIMARY KEY (id)) engine=akibadb;");

        assertTables("user tables",
                "CREATE TABLE %s.one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS("create table `akiba_objects`.`_akiba_one`(`one$id` int ,  INDEX _akiba_one$PK_1(`one$id`)) engine = akibadb",
                "create database if not exists `my_schema`",
                "use `my_schema`",
                "CREATE TABLE `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibadb");

        AkibaInformationSchema ais = manager.getAisCopy();
        assertEquals("ais size", 1, ais.getUserTables().size());
        UserTable table = ais.getUserTable(SCHEMA, "one");
        assertEquals("number of index", 1, table.getIndexes().size());
        Index index = table.getIndexes().iterator().next();
        assertTrue("index isn't primary: " + index, index.isPrimaryKey());

        manager.dropTable(SCHEMA, "one");
    }

    @Test
    public void tableWithoutPK() throws Exception {
        createTable(ErrorCode.NO_PRIMARY_KEY, SCHEMA, "CREATE TABLE one (id int) engine=akibadb;");
    }

    @Test
    public void testSelfReferencingTable() throws Exception {
        createTable(ErrorCode.JOIN_TO_UNKNOWN_TABLE, SCHEMA, "CREATE TABLE one (id int, self_id int, PRIMARY KEY (id), " +
                "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_a` (`one_id`) REFERENCES one (id) ) engine=akibadb;");
    }

    @Test
    public void testAddDropTwoTablesTwoGroups() throws Exception {
        createTable(SCHEMA, "CREATE TABLE one (id int, PRIMARY KEY (id)) engine=akibadb;");

        assertTables("user tables",
                "CREATE TABLE %s.one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS("create table `akiba_objects`.`_akiba_one`(`one$id` int ,  INDEX _akiba_one$PK_1(`one$id`)) engine = akibadb",
                "create database if not exists `my_schema`",
                "use `my_schema`",
                "CREATE TABLE `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibadb");

        createTable(SCHEMA, "CREATE TABLE two (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertTables("user tables",
                "CREATE TABLE %s.one (id int, PRIMARY KEY (id)) engine=akibadb;",
                "CREATE TABLE %s.two (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS("create table `akiba_objects`.`_akiba_one`(`one$id` int ,  INDEX _akiba_one$PK_1(`one$id`)) engine = akibadb",
                "create table `akiba_objects`.`_akiba_two`(`two$id` int ,  INDEX _akiba_two$PK_1(`two$id`)) engine = akibadb",
                "create database if not exists `my_schema`",
                "use `my_schema`",
                "CREATE TABLE `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibadb",
                "CREATE TABLE `my_schema`.two (id int, PRIMARY KEY (id)) engine=akibadb");

        manager.dropTable(SCHEMA, "one");
        assertTables("user tables",
                "CREATE TABLE %s.two (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS("create table `akiba_objects`.`_akiba_two`(`two$id` int ,  INDEX _akiba_two$PK_1(`two$id`)) engine = akibadb",
                "create database if not exists `my_schema`",
                "use `my_schema`",
                "CREATE TABLE `my_schema`.two (id int, PRIMARY KEY (id)) engine=akibadb");
        
        manager.dropTable(SCHEMA, "two");
    }

    @Test
    public void testDropAllTables() throws Exception{

        createTable(SCHEMA, "CREATE TABLE one (id int, PRIMARY KEY (id)) engine=akibadb;");

        assertTables("user tables",
                "CREATE TABLE %s.one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS("create table `akiba_objects`.`_akiba_one`(`one$id` int ,  INDEX _akiba_one$PK_1(`one$id`)) engine = akibadb",
                "create database if not exists `my_schema`",
                "use `my_schema`",
                "CREATE TABLE `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibadb");

        createTable(SCHEMA, "CREATE TABLE two (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertTables("user tables",
                "CREATE TABLE %s.one (id int, PRIMARY KEY (id)) engine=akibadb;",
                "CREATE TABLE %s.two (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS("create table `akiba_objects`.`_akiba_one`(`one$id` int ,  INDEX _akiba_one$PK_1(`one$id`)) engine = akibadb",
                "create table `akiba_objects`.`_akiba_two`(`two$id` int ,  INDEX _akiba_two$PK_1(`two$id`)) engine = akibadb",
                "create database if not exists `my_schema`",
                "use `my_schema`",
                "CREATE TABLE `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibadb",
                "CREATE TABLE `my_schema`.two (id int, PRIMARY KEY (id)) engine=akibadb");

        manager.dropAllTables();
    }

    @Test
    public void testAddDropTwoTablesOneGroupDropRoot() throws Exception {
        createTable(SCHEMA, "CREATE TABLE one (id int, PRIMARY KEY (id)) engine=akibadb;");

        assertTables("user tables",
                "CREATE TABLE %s.one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS("create table `akiba_objects`.`_akiba_one`(`one$id` int ,  INDEX _akiba_one$PK_1(`one$id`)) engine = akibadb",
                "create database if not exists `my_schema`",
                "use `my_schema`",
                "CREATE TABLE `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibadb");

        createTable(SCHEMA, "CREATE TABLE two (id int, one_id int, PRIMARY KEY (id), " +
                "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_a` (`one_id`) REFERENCES one (id) ) engine=akibadb;");
        assertTables("user tables",
                "CREATE TABLE %s.one (id int, PRIMARY KEY (id)) engine=akibadb;",
                "CREATE TABLE %s.two (id int, one_id int, PRIMARY KEY (id), " +
                        "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_a` (`one_id`) REFERENCES one (id) ) engine=akibadb;");
        assertDDLS("create table `akiba_objects`.`_akiba_one`(`one$id` int, `two$id` int, `two$one_id` int ,  "
                   +"INDEX _akiba_one$PK_1(`one$id`), INDEX _akiba_one$PK_2(`two$id`), INDEX two$__akiban_fk_a(`two$one_id`)) engine = akibadb",
                "create database if not exists `my_schema`",
                "use `my_schema`",
                "CREATE TABLE `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibadb",
                "CREATE TABLE `my_schema`.two (id int, one_id int, PRIMARY KEY (id), " +
                        "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_a` (`one_id`) REFERENCES one (id) ) engine=akibadb");

        AkibaInformationSchema ais = manager.getAisCopy();
        assertEquals("ais size", 2, ais.getUserTables().size());
        UserTable table = ais.getUserTable(SCHEMA, "two");
        assertEquals("number of index", 2, table.getIndexes().size());
        Index primaryIndex = table.getIndex("PRIMARY");
        assertTrue("index isn't primary: " + primaryIndex + " in " + table.getIndexes(), primaryIndex.isPrimaryKey());
        Index fkIndex = table.getIndex("__akiban_fk_a");
        assertEquals("fk index name" + " in " + table.getIndexes(), "__akiban_fk_a", fkIndex.getIndexName().getName());

        manager.dropTable(SCHEMA, "one");
    }

    @Test
    public void addChildToNonExistentParent() throws Exception{
        createTable(SCHEMA, "CREATE TABLE one (id int, PRIMARY KEY (id)) engine=akibadb;");

        assertTables("user tables", "CREATE TABLE %s.one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS("create table `akiba_objects`.`_akiba_one`(`one$id` int ,  INDEX _akiba_one$PK_1(`one$id`)) engine = akibadb",
                "create database if not exists `my_schema`",
                "use `my_schema`",
                "CREATE TABLE `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibadb");

        createTable(ErrorCode.JOIN_TO_UNKNOWN_TABLE, SCHEMA, "CREATE TABLE two (id int, one_id int, PRIMARY KEY (id), " +
                "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`one_id`) REFERENCES zebra (id) ) engine=akibadb;");

        assertTables("user tables",
                "CREATE TABLE %s.one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS("create table `akiba_objects`.`_akiba_one`(`one$id` int ,  INDEX _akiba_one$PK_1(`one$id`)) engine = akibadb",
                "create database if not exists `my_schema`",
                "use `my_schema`",
                "CREATE TABLE `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibadb");

        manager.dropTable(SCHEMA, "one");
    }

    @Test
    public void addChildToNonExistentColumns() throws Exception{
        createTable(SCHEMA, "CREATE TABLE one (id int, PRIMARY KEY (id)) engine=akibadb;");

        assertTables("user tables", "CREATE TABLE %s.one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS("create table `akiba_objects`.`_akiba_one`(`one$id` int ,  INDEX _akiba_one$PK_1(`one$id`)) engine = akibadb",
                "create database if not exists `my_schema`",
                "use `my_schema`",
                "CREATE TABLE `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibadb");

        createTable(ErrorCode.JOIN_TO_WRONG_COLUMNS, SCHEMA, "CREATE TABLE two (id int, one_id int, PRIMARY KEY (id), " +
                "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`one_id`) REFERENCES one (invalid_id) ) engine=akibadb;");

        assertTables("user tables", "CREATE TABLE %s.one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS("create table `akiba_objects`.`_akiba_one`(`one$id` int ,  INDEX _akiba_one$PK_1(`one$id`)) engine = akibadb",
                "create database if not exists `my_schema`",
                "use `my_schema`",
                "CREATE TABLE `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibadb");

        manager.dropTable(SCHEMA, "one");
    }

    @Test
    public void addChildToProtectedTable() throws Exception {
        createTable(ErrorCode.JOIN_TO_PROTECTED_TABLE, SCHEMA, "CREATE TABLE one (id int, one_id int, PRIMARY KEY (id), " +
                "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`one_id`) REFERENCES akiba_information_schema.tables (table_id) ) engine=akibadb;");


        createTable(SCHEMA, "CREATE TABLE one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertTables("user tables",
                "CREATE TABLE %s.one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS("create table `akiba_objects`.`_akiba_one`(`one$id` int ,  INDEX _akiba_one$PK_1(`one$id`)) engine = akibadb",
                "create database if not exists `my_schema`",
                "use `my_schema`",
                "CREATE TABLE `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibadb");

        createTable(ErrorCode.JOIN_TO_PROTECTED_TABLE, SCHEMA, "CREATE TABLE two (id int, one_id int, PRIMARY KEY (id), " +
                "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`one_id`) REFERENCES akiba_objects._akiba_one (`one$id`) ) engine=akibadb;");
        assertTables("user tables",
                "CREATE TABLE %s.one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS("create table `akiba_objects`.`_akiba_one`(`one$id` int ,  INDEX _akiba_one$PK_1(`one$id`)) engine = akibadb",
                "create database if not exists `my_schema`",
                "use `my_schema`",
                "CREATE TABLE `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibadb");

        manager.dropTable(SCHEMA, "one");
    }

    @Test
    public void testAddDropTwoTablesOneGroupDropChild() throws Exception {
        createTable(SCHEMA, "CREATE TABLE one (id int, PRIMARY KEY (id)) engine=akibadb;");

        assertTables("user tables",
                "CREATE TABLE %s.one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS("create table `akiba_objects`.`_akiba_one`(`one$id` int ,  INDEX _akiba_one$PK_1(`one$id`)) engine = akibadb",
                "create database if not exists `my_schema`",
                "use `my_schema`",
                "CREATE TABLE `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibadb");

        createTable(SCHEMA, "CREATE TABLE two (id int, one_id int, PRIMARY KEY (id), " +
                "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`one_id`) REFERENCES one (id) ) engine=akibadb;");
        assertTables("user tables",
                "CREATE TABLE %s.one (id int, PRIMARY KEY (id)) engine=akibadb;",
                "CREATE TABLE %s.two (id int, one_id int, PRIMARY KEY (id), " +
                        "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`one_id`) REFERENCES one (id) ) engine=akibadb;");
        assertDDLS("create table `akiba_objects`.`_akiba_one`(`one$id` int, `two$id` int, `two$one_id` int ,  "
                   +"INDEX _akiba_one$PK_1(`one$id`), INDEX _akiba_one$PK_2(`two$id`), INDEX two$__akiban_fk_0(`two$one_id`)) engine = akibadb",
                "create database if not exists `my_schema`",
                "use `my_schema`",
                "CREATE TABLE `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibadb",
                "CREATE TABLE `my_schema`.two (id int, one_id int, PRIMARY KEY (id), " +
                        "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`one_id`) REFERENCES one (id) ) engine=akibadb");

        manager.dropTable(SCHEMA, "two");
        // Commenting out the following as a fix to bug 188. We're now dropping whole groups at a time, instead of just
        // branches.
//        assertTables("user tables",
//                "CREATE TABLE %s.one (id int, PRIMARY KEY (id)) engine=akibadb;");
//        assertDDLS("create table `akiba_objects`.`_akiba_one`(`one$id` int ,  INDEX _akiba_one$one_PK(`one$id`)) engine = akibadb",
//                "create database if not exists `my_schema`",
//                "use `my_schema`",
//                "CREATE TABLE `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibadb");

        // Commenting out the following as a fix to bug 188. We're now dropping whole groups at a time, instead of just
        // branches.
//        manager.dropTable(SCHEMA, "one");
//        assertTables("user tables");
//        assertDDLS();
    }

    @Test
    public void dropNonExistentTable() throws Exception {
        manager.dropTable("this_schema_does_not", "exist");
        
        createTable(SCHEMA, "CREATE TABLE one (id int, PRIMARY KEY (id)) engine=akibadb;");

        assertTables("user tables",
                "CREATE TABLE %s.one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS("create table `akiba_objects`.`_akiba_one`(`one$id` int ,  INDEX _akiba_one$PK_1(`one$id`)) engine = akibadb",
                "create database if not exists `my_schema`",
                "use `my_schema`",
                "CREATE TABLE `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibadb");

        manager.dropTable(SCHEMA, "one");
        manager.dropTable(SCHEMA, "one");

        manager.dropTable("this_schema_never_existed", "it_really_didnt");
        manager.dropTable("this_schema_never_existed", "it_really_didnt");
    }

    @Test
    public void overloadTableAndColumn() throws Exception {
        // we don't allow two tables s1.foo and s2.foo to have any identical columns
        // But we do want to allow same-name tables in different schemas if they don't share any columns
        List<String> expectedDDLs = Collections.unmodifiableList(Arrays.asList(
                "create table `akiba_objects`.`_akiba_one`(`one$idFoo` int ,  INDEX _akiba_one$PK_1(`one$idFoo`)) engine = akibadb",
                "create database if not exists `s1`",
                "use `s1`",
                "CREATE TABLE `s1`.one (idFoo int, PRIMARY KEY (idFoo)) engine=akibadb"));

        createTable("s1", "CREATE TABLE one (idFoo int, PRIMARY KEY (idFoo)) engine=akibadb;");
        assertTables("user tables",
                "CREATE TABLE `s1`.one (idFoo int, PRIMARY KEY (idFoo)) engine=akibadb;");
        assertDDLS(expectedDDLs.toArray(new String[expectedDDLs.size()]));

        List<String> expectedDDLs2 = new ArrayList<String>(expectedDDLs);
        expectedDDLs2.add(0, "create table `akiba_objects`.`_akiba_one$0`(`one$id` int ,  INDEX _akiba_one$0$PK_1(`one$id`)) engine = akibadb");
        expectedDDLs2.add("create database if not exists `s2`");
        expectedDDLs2.add("use `s2`");
        expectedDDLs2.add("CREATE TABLE `s2`.one (id int, PRIMARY KEY (id)) engine=akibadb");
        createTable("s2", "CREATE TABLE one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertTables("user tables",
                "CREATE TABLE `s1`.one (idFoo int, PRIMARY KEY (idFoo)) engine=akibadb;",
                "CREATE TABLE `s2`.one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS(expectedDDLs2.toArray(new String[expectedDDLs.size()]));

        // No changes when trying to add a table like s2.one
        createTable(ErrorCode.DUPLICATE_COLUMN_NAMES, "s3", "CREATE TABLE one (id int, PRIMARY KEY (id)) engine=akibadb;");
        manager.getAisCopy();
        assertTables("user tables",
                "CREATE TABLE `s1`.one (idFoo int, PRIMARY KEY (idFoo)) engine=akibadb;",
                "CREATE TABLE `s2`.one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS(expectedDDLs2.toArray(new String[expectedDDLs.size()]));

        manager.dropTable("s2", "one");
        List<String> expectedDDLs3 = new ArrayList<String>(expectedDDLs);
        expectedDDLs3.add(0, "create table `akiba_objects`.`_akiba_one$0`(`one$id` int ,  INDEX _akiba_one$0$PK_1(`one$id`)) engine = akibadb");
        expectedDDLs3.add("create database if not exists `s3`");
        expectedDDLs3.add("use `s3`");
        expectedDDLs3.add("CREATE TABLE `s3`.one (id int, PRIMARY KEY (id)) engine=akibadb");
        createTable("s3", "CREATE TABLE one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertTables("user tables",
                "CREATE TABLE `s1`.one (idFoo int, PRIMARY KEY (idFoo)) engine=akibadb;",
                "CREATE TABLE `s3`.one (id int, PRIMARY KEY (id)) engine=akibadb;");
        assertDDLS(expectedDDLs3.toArray(new String[expectedDDLs.size()]));

        manager.dropTable("s3", "one");
        manager.dropTable("s1", "one");
    }

    private void assertTables(String message, String... expecteds) throws Exception {
        Map<TableName,String>  actual = manager.getUserTables();
        Map<TableName,String> expMap = new HashMap<TableName, String>(actual.size());
        for (String expected : expecteds) {
            expected = String.format(expected, '`' + SCHEMA + '`');
            Matcher m = REGEX.matcher(expected);
            assertTrue("regex not found in " + expected, m.find());
            TableName table = TableName.create(m.group(1), m.group(2));
            expMap.put(table, expected);
        }
        assertEquals(message, expMap, new HashMap<TableName,String>(actual));
    }

    private void assertDDLS(String... expected) throws Exception{
        List<String> expectedList = new ArrayList<String>();
        expectedList.add("create database if not exists `akiba_information_schema`");
        expectedList.add("use `akiba_information_schema`");
        expectedList.add("create table groups(     group_name varchar(64),     primary key(group_name) ) engine = akibadb");
        expectedList.add("create table tables(     schema_name       varchar(64),     table_name        varchar(64),     table_type        varchar(8),     table_id          int,     group_name        varchar(64),     source_types      int,     primary key(schema_name, table_name) ) engine = akibadb");
        expectedList.add("create table columns (     schema_name         varchar(64),     table_name          varchar(64),     column_name         varchar(64),     position            int,      type                varchar(64),     type_param_1        bigint,     type_param_2        bigint,     nullable            tinyint,     initial_autoinc     bigint,     group_schema_name   varchar(64),     group_table_name    varchar(64),     group_column_name   varchar(64),     maximum_size		bigint,     prefix_size			int,     character_set       varchar(32),     collation           varchar(32),     primary key(schema_name, table_name, column_name) ) engine = akibadb");
        expectedList.add("create table joins(     join_name               varchar(767),     parent_schema_name      varchar(64),     parent_table_name       varchar(64),     child_schema_name       varchar(64),     child_table_name        varchar(64),     group_name              varchar(64),     join_weight             int,     grouping_usage          int,     source_types            int,     primary key(join_name) ) engine = akibadb");
        expectedList.add("create table join_columns(     join_name               varchar(767),     parent_schema_name      varchar(64),     parent_table_name       varchar(64),     parent_column_name      varchar(64),     child_schema_name       varchar(64),     child_table_name        varchar(64),     child_column_name       varchar(64),     primary key(join_name, parent_column_name, child_column_name) ) engine = akibadb");
        expectedList.add("create table indexes (     schema_name      varchar(64),     table_name       varchar(64),     index_name       varchar(64),     index_id         int,     table_constraint varchar(64),     is_unique        tinyint,     primary key(schema_name, table_name, index_name) ) engine = akibadb");
        expectedList.add("create table index_columns (     schema_name       varchar(64),     table_name        varchar(64),     index_name        varchar(64),     column_name       varchar(64),     ordinal_position  int,     is_ascending      tinyint,     indexed_length    int,     primary key(schema_name, table_name, index_name, column_name) ) engine = akibadb");
        expectedList.add("create table types(     type_name           varchar(64),     parameters          int,     fixed_size          tinyint,     max_size_bytes      bigint,     primary key(type_name) ) engine = akibadb");
        expectedList.add("create table index_analysis(     table_id            int,     index_id            int,     analysis_timestamp  timestamp,     item_number         int,     key_string          varchar(2048),     index_row_data      varbinary(4096),     count               bigint,     primary key(table_id, index_id, item_number) ) engine = akibadb");
        expectedList.add("create schema if not exists `akiba_objects`");

        expectedList.addAll(Arrays.asList(expected));
        String actual = Strings.join(manager.getDDLs());
        String expectedStr = Strings.join(expectedList);
        assertEquals("DDLs", expectedStr, actual);
    }
}
