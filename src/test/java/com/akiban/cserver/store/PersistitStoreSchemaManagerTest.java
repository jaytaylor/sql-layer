package com.akiban.cserver.store;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.CServer;
import com.akiban.cserver.CServerTestCase;
import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.service.config.Property;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.message.ErrorCode;
import com.akiban.util.MySqlStatementSplitter;

public final class PersistitStoreSchemaManagerTest extends CServerTestCase {

    private final static String AIS_CREATE_STATEMENTS = readAisSchema();
    private final static String SCHEMA = "my_schema";
    private final static Pattern REGEX = Pattern.compile("create table `(\\w+)`\\.(\\w+)");

    private int base;
    
    SchemaManager manager;
 
    @Before
    public void setUp() throws Exception {
        // Set up multi-volume treespace policy so we can be sure schema is
        // properly distributed.
        final Collection<Property> properties = new ArrayList<Property>();
        properties.add(property("cserver", "treespace.a",
                "drupal*:${datapath}/${schema}.v0,create,pageSize:8K,"
                        + "initialSize:10K,extensionSize:1K,maximumSize:10G"));
        properties.add(property("cserver", "treespace",
                "liveops*:${datapath}/${schema}.v0,create,pageSize:8K,"
                        + "initialSize:10K,extensionSize:1K,maximumSize:10G"));
        baseSetUp();
        manager = getSchemaManager();
        base = manager.getAis(session).getUserTables().size();
        assertTables("user tables");
        assertDDLS();
    }

    @After
    public void tearDown() throws Exception {
        try {
            assertEquals("user tables in AIS", base, manager.getAis(session).getUserTables().size());
            assertTables("user tables");
            assertDDLS();
        } finally {
            baseTearDown();
        }
    }

    private void createTable(ErrorCode expectedCode, String schema, String ddl) throws Exception {
        ErrorCode actualCode  = null;
        try {
            manager.createTableDefinition(session, schema, ddl, false);
        }
        catch (InvalidOperationException e) {
            actualCode = e.getCode();
        }
        assertEquals("createTable return value", expectedCode, actualCode);
    }
    
    private void createTable(String schema, String ddl) throws Exception {
        manager.createTableDefinition(session, schema, ddl, false);
    }

    
    @Test
    public void testUtf8Table() throws Exception {
        createTable(SCHEMA,
                "create table myvarchartest1(id int key, name varchar(85) character set UTF8) engine=akibandb");
        createTable(SCHEMA,
                "create table myvarchartest2(id int key, name varchar(86) character set utf8) engine=akibandb");
        AkibaInformationSchema ais = manager.getAis(session);
        Column c1 = ais.getTable(SCHEMA, "myvarchartest1").getColumn("name");
        Column c2 = ais.getTable(SCHEMA, "myvarchartest2").getColumn("name");
        assertEquals("UTF8", c1.getCharsetAndCollation().charset());
        assertEquals("utf8", c2.getCharsetAndCollation().charset());
// 
//  See bug 337 - reenable these asserts after 337 is fixed.
        assertEquals(Integer.valueOf(1), c1.getPrefixSize());
        assertEquals(Integer.valueOf(2), c2.getPrefixSize());
        manager.deleteTableDefinition(session, SCHEMA, "myvarchartest1");
        manager.deleteTableDefinition(session, SCHEMA, "myvarchartest2");
    }

    @Test
    public void testAddDropOneTable() throws Exception {

        createTable(SCHEMA, "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");

        assertTables("user tables",
                "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS(
                "create schema if not exists `my_schema`",
                "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        AkibaInformationSchema ais = manager.getAis(session);
        assertEquals("ais size", base + 1, ais.getUserTables().size());
        UserTable table = ais.getUserTable(SCHEMA, "one");
        assertEquals("number of index", 1, table.getIndexes().size());
        Index index = table.getIndexes().iterator().next();
        assertTrue("index isn't primary: " + index, index.isPrimaryKey());

        manager.deleteTableDefinition(session, SCHEMA, "one");
    }

    @Test
    public void testSelfReferencingTable() throws Exception {
        createTable(ErrorCode.JOIN_TO_UNKNOWN_TABLE, SCHEMA, "create table one (id int, self_id int, PRIMARY KEY (id), " +
                "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_a` (`one_id`) REFERENCES one (id) ) engine=akibandb;");
    }

    @Test
    public void noEngineName() throws Exception {
        createTable(SCHEMA, "create table zebra( id int key)");
        assertDDLS("create schema if not exists `my_schema`",
                "create table `my_schema`.zebra( id int key)");
        manager.deleteTableDefinition(session, SCHEMA, "zebra");
    }

    @Test
    public void testAddDropTwoTablesTwoGroups() throws Exception {
        createTable(SCHEMA, "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");

        assertTables("user tables",
                "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `my_schema`",
                "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        createTable(SCHEMA, "create table two (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertTables("user tables",
                "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;",
                "create table %s.two (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `my_schema`",
                "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb",
                "create table `my_schema`.two (id int, PRIMARY KEY (id)) engine=akibandb");

        manager.deleteTableDefinition(session, SCHEMA, "one");
        assertTables("user tables",
                "create table %s.two (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `my_schema`",
                "create table `my_schema`.two (id int, PRIMARY KEY (id)) engine=akibandb");
        
        manager.deleteTableDefinition(session, SCHEMA, "two");
    }

    // TODO This test proves that dropping a parent table definition also drops the child.
    // Still in play. Or at least I want to have the discussion.
    @Test
    public void testAddDropTwoTablesOneGroupDropRoot() throws Exception {
        createTable(SCHEMA, "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");

        assertTables("user tables",
                "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `my_schema`",
                "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        createTable(SCHEMA, "create table two (id int, one_id int, PRIMARY KEY (id), " +
                "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_a` (`one_id`) REFERENCES one (id) ) engine=akibandb;");
        assertTables("user tables",
                "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;",
                "create table %s.two (id int, one_id int, PRIMARY KEY (id), " +
                        "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_a` (`one_id`) REFERENCES one (id) ) engine=akibandb;");
        assertDDLS("create schema if not exists `my_schema`",
                "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb",
                "create table `my_schema`.two (id int, one_id int, PRIMARY KEY (id), " +
                        "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_a` (`one_id`) REFERENCES one (id) ) engine=akibandb");

        AkibaInformationSchema ais = manager.getAis(session);
        assertEquals("ais size", base + 2, ais.getUserTables().size());
        UserTable table = ais.getUserTable(SCHEMA, "two");
        assertEquals("number of index", 2, table.getIndexes().size());
        Index primaryIndex = table.getIndex(Index.PRIMARY_KEY_CONSTRAINT);
        assertTrue("index isn't primary: " + primaryIndex + " in " + table.getIndexes(), primaryIndex.isPrimaryKey());
        Index fkIndex = table.getIndex("__akiban_fk_0");
        assertEquals("fk index name" + " in " + table.getIndexes(), "__akiban_fk_0", fkIndex.getIndexName().getName());

        manager.deleteTableDefinition(session, SCHEMA, "one");
    }

    @Test
    public void addChildToNonExistentParent() throws Exception{
        createTable(SCHEMA, "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");

        assertTables("user tables", "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `my_schema`",
                "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        createTable(ErrorCode.JOIN_TO_UNKNOWN_TABLE, SCHEMA, "create table two (id int, one_id int, PRIMARY KEY (id), " +
                "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`one_id`) REFERENCES zebra (id) ) engine=akibandb;");

        assertTables("user tables",
                "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `my_schema`",
                "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        manager.deleteTableDefinition(session, SCHEMA, "one");
    }

    @Test
    public void addChildToNonExistentColumns() throws Exception{
        createTable(SCHEMA, "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");

        assertTables("user tables", "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `my_schema`",
                "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        createTable(ErrorCode.JOIN_TO_WRONG_COLUMNS, SCHEMA, "create table two (id int, one_id int, PRIMARY KEY (id), " +
                "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`one_id`) REFERENCES one (invalid_id) ) engine=akibandb;");

        assertTables("user tables", "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `my_schema`",
                "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        manager.deleteTableDefinition(session, SCHEMA, "one");
    }

    @Test
    public void addChildToProtectedTable() throws Exception {
        createTable(ErrorCode.JOIN_TO_PROTECTED_TABLE, SCHEMA, "create table one (id int, one_id int, PRIMARY KEY (id), " +
                "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`one_id`) REFERENCES akiba_information_schema.tables (table_id) ) engine=akibandb;");


        createTable(SCHEMA, "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertTables("user tables",
                "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `my_schema`",
                "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");
        assertTables("user tables",
                "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `my_schema`",
                "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        manager.deleteTableDefinition(session, SCHEMA, "one");
    }

    
    // TODO: This test proves that dropping a child table definition also drops the parent.
    // Still in play. Or at least I want to have the discussion.
    @Test
    public void testAddDropTwoTablesOneGroupDropChild() throws Exception {
        createTable(SCHEMA, "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");

        assertTables("user tables",
                "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `my_schema`",
                "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        createTable(SCHEMA, "create table two (id int, one_id int, PRIMARY KEY (id), " +
                "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`one_id`) REFERENCES one (id) ) engine=akibandb;");
        assertTables("user tables",
                "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;",
                "create table %s.two (id int, one_id int, PRIMARY KEY (id), " +
                        "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`one_id`) REFERENCES one (id) ) engine=akibandb;");
        assertDDLS("create schema if not exists `my_schema`",
                "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb",
                "create table `my_schema`.two (id int, one_id int, PRIMARY KEY (id), " +
                        "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`one_id`) REFERENCES one (id) ) engine=akibandb");

        manager.deleteTableDefinition(session, SCHEMA, "two");
        // Commenting out the following as a fix to bug 188. We're now dropping whole groups at a time, instead of just
        // branches.
//        assertTables("user tables",
//                "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;");
//        assertDDLS("create schema if not exists `my_schema`",
//                "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        // Commenting out the following as a fix to bug 188. We're now dropping whole groups at a time, instead of just
        // branches.
//        manager.deleteTableDefinition(SCHEMA, "one");
//        assertTables("user tables");
//        assertDDLS();
    }

    @Test
    public void dropNonExistentTable() throws Exception {
        manager.deleteTableDefinition(session, "this_schema_does_not", "exist");
        
        createTable(SCHEMA, "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");

        assertTables("user tables",
                "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `my_schema`",
                "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        manager.deleteTableDefinition(session, SCHEMA, "one");
        manager.deleteTableDefinition(session, SCHEMA, "one");

        manager.deleteTableDefinition(session, "this_schema_never_existed", "it_really_didnt");
        manager.deleteTableDefinition(session, "this_schema_never_existed", "it_really_didnt");
    }
    
    @Test
    public void testAddDropTwoTablesTwoVolumes() throws Exception {
        AkibaInformationSchema ais;
        Session session = new SessionImpl();
        createTable("drupal_a", "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `drupal_a`",
                "create table `drupal_a`.one (id int, PRIMARY KEY (id)) engine=akibandb");
        ais = manager.getAis(session);
        assertNotNull( ais.getUserTable("drupal_a", "one"));
        createTable("drupal_b", "create table two (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `drupal_a`",
                "create table `drupal_a`.one (id int, PRIMARY KEY (id)) engine=akibandb",
                "create schema if not exists `drupal_b`",
                "create table `drupal_b`.two (id int, PRIMARY KEY (id)) engine=akibandb");
        ais = manager.getAis(session);
        assertNotNull( ais.getUserTable("drupal_a", "one"));
        assertNotNull( ais.getUserTable("drupal_b", "two"));
        manager.deleteTableDefinition(session, "drupal_a", "one");
        assertDDLS("create schema if not exists `drupal_b`",
                "create table `drupal_b`.two (id int, PRIMARY KEY (id)) engine=akibandb");
        ais = manager.getAis(session);
        assertNull( ais.getUserTable("drupal_a", "one"));
        assertNotNull( ais.getUserTable("drupal_b", "two"));
        manager.deleteTableDefinition(session, "drupal_b", "two");
    }



    private void assertTables(String message, String... expecteds) throws Exception {
        Collection<TableDefinition> definitions = schemaManager.getTableDefinitions(session, SCHEMA).values();
        Map<TableName,String>  actual = new HashMap<TableName, String>();
        for (TableDefinition td : definitions) {
            TableName tn = new TableName(td.getSchemaName(), td.getTableName());
            actual.put(tn, td.getDDL());
        }
        
        
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

    private void assertDDLS(String... statements) throws Exception{
        StringBuilder sb = new StringBuilder(AIS_CREATE_STATEMENTS);
        for (final String s : statements) {
            sb.append(s).append(";").append(CServerUtil.NEW_LINE);
        }
        final String expected = sb.toString();
        final String actual = manager.schemaString(session, false);
        assertEquals("DDLs", expected, actual);
    }
    
    private static String readAisSchema() {
        final StringBuilder sb = new StringBuilder();
        sb.append("create schema if not exists `akiba_information_schema`;");
            sb.append(CServerUtil.NEW_LINE);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(
                CServer.class.getClassLoader()
                        .getResourceAsStream(PersistitStoreSchemaManager.AIS_DDL_NAME)));
        for (String statement : (new MySqlStatementSplitter(reader))) {
            final String canonical = DDLSource.canonicalStatement(statement);
            sb.append(canonical);
            sb.append(CServerUtil.NEW_LINE);
        }
        return sb.toString();
    }


}
