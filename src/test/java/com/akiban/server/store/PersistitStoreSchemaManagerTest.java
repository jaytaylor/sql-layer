/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.akiban.ais.ddl.SchemaDef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.message.ErrorCode;
import com.akiban.server.AkServer;
import com.akiban.server.AkServerTestCase;
import com.akiban.server.AkServerUtil;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.service.config.Property;
import com.akiban.util.MySqlStatementSplitter;

public final class PersistitStoreSchemaManagerTest extends AkServerTestCase {

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
        properties.add(property("akserver.treespace.a",
                "drupal*:${datapath}/${schema}.v0,create,pageSize:8K,"
                        + "initialSize:10K,extensionSize:1K,maximumSize:10G"));
        properties.add(property("akserver.treespace",
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
        AkibanInformationSchema ais = manager.getAis(session);
        Column c1 = ais.getTable(SCHEMA, "myvarchartest1").getColumn("name");
        Column c2 = ais.getTable(SCHEMA, "myvarchartest2").getColumn("name");
        assertEquals("UTF8", c1.getCharsetAndCollation().charset());
        assertEquals("utf8", c2.getCharsetAndCollation().charset());

        assertEquals(Integer.valueOf(1), c1.getPrefixSize());
        assertEquals(Integer.valueOf(2), c2.getPrefixSize());
        manager.deleteTableDefinition(session, SCHEMA, "myvarchartest1");
        manager.deleteTableDefinition(session, SCHEMA, "myvarchartest2");
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
                "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`one_id`) REFERENCES akiban_information_schema.tables (table_id) ) engine=akibandb;");


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

    @Test
    public void testDeleteOneDefinition() throws Exception {
        createTable(SCHEMA, "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");

        assertTables("user tables",
                     "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `my_schema`",
                   "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        AkibanInformationSchema ais = manager.getAis(session);
        assertEquals("ais size", base + 1, ais.getUserTables().size());
        UserTable table = ais.getUserTable(SCHEMA, "one");
        assertEquals("number of index", 1, table.getIndexes().size());
        Index index = table.getIndexes().iterator().next();
        assertTrue("index isn't primary: " + index, index.isPrimaryKey());

        manager.deleteTableDefinition(session, SCHEMA, "one");
    }
    
    @Test
    public void testBug712605() throws Exception {
        long time = System.currentTimeMillis();
        // try this for 10 seconds.
        while (System.currentTimeMillis() - time < 20000) {
            createTable(SCHEMA, "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");
            manager.deleteTableDefinition(session, SCHEMA, "one");
        }
    }

    @Test
    public void testDeleteDefinitionNonExistentTable() throws Exception {
        manager.deleteTableDefinition(session, "this_schema_does_not", "exist");

        createTable(SCHEMA, "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");

        assertTables("user tables",
                     "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `my_schema`",
                   "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        manager.deleteTableDefinition(session, SCHEMA, "one");
        assertTrue("table still exists", manager.getTableDefinitions(session, SCHEMA).isEmpty());

        manager.deleteTableDefinition(session, SCHEMA, "one");

        manager.deleteTableDefinition(session, "this_schema_never_existed", "it_really_didnt");
        manager.deleteTableDefinition(session, "this_schema_never_existed", "it_really_didnt");
    }

    @Test
    public void testDeleteDefinitionTwoTablesTwoGroups() throws Exception {
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

    @Test
    public void testDeleteDefinitionTwoTablesOneGroupDeleteChild() throws Exception {
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

        // Deleting child should not delete parent
        manager.deleteTableDefinition(session, SCHEMA, "two");

        assertTables("user tables",
                     "create table %s.one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `my_schema`",
                   "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        manager.deleteTableDefinition(session, SCHEMA, "one");
    }

    @Test
    public void testDeleteDefinitionTwoTablesOneGroupDeleteParent() throws Exception {
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

        AkibanInformationSchema ais = manager.getAis(session);
        assertEquals("ais size", base + 2, ais.getUserTables().size());
        UserTable table = ais.getUserTable(SCHEMA, "two");
        assertEquals("number of index", 2, table.getIndexes().size());
        Index primaryIndex = table.getIndex(Index.PRIMARY_KEY_CONSTRAINT);
        assertTrue("index isn't primary: " + primaryIndex + " in " + table.getIndexes(), primaryIndex.isPrimaryKey());
        Index fkIndex = table.getIndex("__akiban_fk_0");
        assertEquals("fk index name" + " in " + table.getIndexes(), "__akiban_fk_0", fkIndex.getIndexName().getName());

        try {
            // Deleting parent should be rejected
            manager.deleteTableDefinition(session, SCHEMA, "one");
            assertTrue("exception thrown", false);
        } catch(InvalidOperationException e) {
            // Expected (as try/catch since tearDown requires tables removed)
            assertEquals("error code", ErrorCode.UNSUPPORTED_MODIFICATION, e.getCode());
        }

        manager.deleteTableDefinition(session, SCHEMA, "two");
        manager.deleteTableDefinition(session, SCHEMA, "one");
    }

    @Test
    public void testDeleteDefinitionTwoTablesTwoVolumes() throws Exception {
        AkibanInformationSchema ais;

        createTable("drupal_a", "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `drupal_a`",
                   "create table `drupal_a`.one (id int, PRIMARY KEY (id)) engine=akibandb");
        ais = manager.getAis(session);
        assertNotNull(ais.getUserTable("drupal_a", "one"));

        createTable("drupal_b", "create table two (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `drupal_a`",
                   "create table `drupal_a`.one (id int, PRIMARY KEY (id)) engine=akibandb",
                   "create schema if not exists `drupal_b`",
                   "create table `drupal_b`.two (id int, PRIMARY KEY (id)) engine=akibandb");
        ais = manager.getAis(session);
        assertNotNull(ais.getUserTable("drupal_a", "one"));
        assertNotNull(ais.getUserTable("drupal_b", "two"));

        manager.deleteTableDefinition(session, "drupal_a", "one");
        assertDDLS("create schema if not exists `drupal_b`",
                   "create table `drupal_b`.two (id int, PRIMARY KEY (id)) engine=akibandb");
        ais = manager.getAis(session);
        assertNull(ais.getUserTable("drupal_a", "one"));
        assertNotNull(ais.getUserTable("drupal_b", "two"));
        
        manager.deleteTableDefinition(session, "drupal_b", "two");
        assertNull(ais.getUserTable("drupal_a", "two"));
    }

    @Test
    public void badCharsetIsRejected() throws Exception {
        // Table level
        try {
            createTable(SCHEMA, "create table t(id int key) engine=akibandb default charset=banana;");
            Assert.fail("Expected InvalidOperationException (unsupported charset)");
        } catch(InvalidOperationException e) {
            assertEquals(ErrorCode.UNSUPPORTED_CHARSET, e.getCode());
        }
        // Column level
        try {
            createTable(SCHEMA, "create table t(name varchar(32) charset utf42) engine=akibandb;");
            Assert.fail("Expected InvalidOperationException (unsupported charset)");
        } catch(InvalidOperationException e) {
            assertEquals(ErrorCode.UNSUPPORTED_CHARSET, e.getCode());
        }
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
            sb.append(s).append(";").append(AkServerUtil.NEW_LINE);
        }
        final String expected = sb.toString();
        final String actual = manager.schemaString(session, false);
        assertEquals("DDLs", expected, actual);
    }
    
    private static String readAisSchema() {
        final StringBuilder sb = new StringBuilder();
        sb.append("create schema if not exists `akiban_information_schema`;");
            sb.append(AkServerUtil.NEW_LINE);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(
                AkServer.class.getClassLoader()
                        .getResourceAsStream(PersistitStoreSchemaManager.AIS_DDL_NAME)));
        for (String statement : (new MySqlStatementSplitter(reader))) {
            final String canonical = SchemaDef.canonicalStatement(statement);
            sb.append(canonical);
            sb.append(AkServerUtil.NEW_LINE);
        }
        return sb.toString();
    }
}
