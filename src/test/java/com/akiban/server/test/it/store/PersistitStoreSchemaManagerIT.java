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

package com.akiban.server.test.it.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import com.akiban.ais.ddl.SchemaDef;
import com.akiban.ais.model.Table;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.TableDefinition;
import com.akiban.server.test.it.ITBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.message.ErrorCode;
import com.akiban.server.AkServer;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.service.config.Property;
import com.akiban.util.MySqlStatementSplitter;

public final class PersistitStoreSchemaManagerIT extends ITBase {

    private final static List<String> AIS_CREATE_STATEMENTS = readAisSchema();
    private final static String SCHEMA = "my_schema";

    private int base;
    private SchemaManager manager;

    @Override
    protected Collection<Property> startupConfigProperties() {
        // Set up multi-volume treespace policy so we can be sure schema is properly distributed.
        final Collection<Property> properties = new ArrayList<Property>();
        properties.add(new Property("akserver.treespace.a",
                                    "drupal*:${datapath}/${schema}.v0,create,pageSize:${buffersize},"
                                    + "initialSize:10K,extensionSize:1K,maximumSize:10G"));
        properties.add(new Property("akserver.treespace",
                                    "liveops*:${datapath}/${schema}.v0,create,pageSize:${buffersize},"
                                            + "initialSize:10K,extensionSize:1K,maximumSize:10G"));
        return properties;
    }

    @Before
    public void setUp() throws Exception {
        manager = serviceManager().getSchemaManager();
        base = manager.getAis(session()).getUserTables().size();
        assertTablesInSchema(SCHEMA);
        assertDDLS();
    }

    @After
    public void tearDown() throws Exception {
        assertEquals("user tables in AIS", base, manager.getAis(session()).getUserTables().size());
        assertTablesInSchema(SCHEMA);
        assertDDLS();
    }
    
    private void createTableDef(String schema, String ddl) throws Exception {
        manager.createTableDefinition(session(), schema, ddl, false);
    }

    private void deleteTableDef(String schema, String table) throws Exception {
        manager.deleteTableDefinition(session(), schema, table);
    }

    private AkibanInformationSchema getAIS() {
        return manager.getAis(session());
    }

    @Test
    public void testDeleteOneDefinition() throws Exception {
        createTableDef(SCHEMA, "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");

        assertTablesInSchema(SCHEMA, "one");
        assertDDLS("create schema if not exists `my_schema`",
                   "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        AkibanInformationSchema ais = getAIS();
        assertEquals("ais size", base + 1, ais.getUserTables().size());
        UserTable table = ais.getUserTable(SCHEMA, "one");
        assertEquals("number of index", 1, table.getIndexes().size());
        Index index = table.getIndexes().iterator().next();
        assertTrue("index isn't primary: " + index, index.isPrimaryKey());

        deleteTableDef(SCHEMA, "one");
    }

    @Test
    public void testDeleteDefinitionNonExistentTable() throws Exception {
        deleteTableDef("this_schema_does_not", "exist");

        createTableDef(SCHEMA, "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");

        assertTablesInSchema(SCHEMA, "one");
        assertDDLS("create schema if not exists `my_schema`",
                   "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        deleteTableDef(SCHEMA, "one");
        assertTrue("table still exists", manager.getTableDefinitions(session(), SCHEMA).isEmpty());

        deleteTableDef(SCHEMA, "one");
        deleteTableDef("this_schema_never_existed", "it_really_didnt");
        deleteTableDef("this_schema_never_existed", "it_really_didnt");
    }

    @Test
    public void testDeleteDefinitionTwoTablesTwoGroups() throws Exception {
        createTableDef(SCHEMA, "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");

        assertTablesInSchema(SCHEMA, "one");
        assertDDLS("create schema if not exists `my_schema`",
                   "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        createTableDef(SCHEMA, "create table two (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertTablesInSchema(SCHEMA, "one", "two");
        assertDDLS("create schema if not exists `my_schema`",
                   "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb",
                   "create table `my_schema`.two (id int, PRIMARY KEY (id)) engine=akibandb");

        deleteTableDef(SCHEMA, "one");
        assertTablesInSchema(SCHEMA, "two");
        assertDDLS("create schema if not exists `my_schema`",
                   "create table `my_schema`.two (id int, PRIMARY KEY (id)) engine=akibandb");

        deleteTableDef(SCHEMA, "two");
    }

    @Test
    public void testDeleteDefinitionTwoTablesOneGroupDeleteChild() throws Exception {
        createTableDef(SCHEMA, "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");

        assertTablesInSchema(SCHEMA, "one");
        assertDDLS("create schema if not exists `my_schema`",
                   "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        createTableDef(SCHEMA, "create table two (id int, one_id int, PRIMARY KEY (id), " +
                "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`one_id`) REFERENCES one (id) ) engine=akibandb;");
        assertTablesInSchema(SCHEMA, "one", "two");
        assertDDLS("create schema if not exists `my_schema`",
                   "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb",
                   "create table `my_schema`.two (id int, one_id int, PRIMARY KEY (id), " +
                           "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`one_id`) REFERENCES one (id) ) engine=akibandb");

        // Deleting child should not delete parent
        deleteTableDef(SCHEMA, "two");

        assertTablesInSchema(SCHEMA, "one");
        assertDDLS("create schema if not exists `my_schema`",
                   "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        deleteTableDef(SCHEMA, "one");
    }

    @Test
    public void testDeleteDefinitionTwoTablesOneGroupDeleteParent() throws Exception {
        createTableDef(SCHEMA, "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");

        assertTablesInSchema(SCHEMA, "one");
        assertDDLS("create schema if not exists `my_schema`",
                   "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb");

        createTableDef(SCHEMA, "create table two (id int, one_id int, PRIMARY KEY (id), " +
                "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_a` (`one_id`) REFERENCES one (id) ) engine=akibandb;");
        assertTablesInSchema(SCHEMA, "one", "two");
        assertDDLS("create schema if not exists `my_schema`",
                   "create table `my_schema`.one (id int, PRIMARY KEY (id)) engine=akibandb",
                   "create table `my_schema`.two (id int, one_id int, PRIMARY KEY (id), " +
                   "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_a` (`one_id`) REFERENCES one (id) ) engine=akibandb");

        AkibanInformationSchema ais = getAIS();
        assertEquals("ais size", base + 2, ais.getUserTables().size());
        UserTable table = ais.getUserTable(SCHEMA, "two");
        assertEquals("number of index", 2, table.getIndexes().size());
        Index primaryIndex = table.getIndex(Index.PRIMARY_KEY_CONSTRAINT);
        assertTrue("index isn't primary: " + primaryIndex + " in " + table.getIndexes(), primaryIndex.isPrimaryKey());
        Index fkIndex = table.getIndex("__akiban_fk_0");
        assertEquals("fk index name" + " in " + table.getIndexes(), "__akiban_fk_0", fkIndex.getIndexName().getName());

        try {
            // Deleting parent should be rejected
            deleteTableDef(SCHEMA, "one");
            assertTrue("exception thrown", false);
        } catch(InvalidOperationException e) {
            // Expected (as try/catch since tearDown requires tables removed)
            assertEquals("error code", ErrorCode.UNSUPPORTED_MODIFICATION, e.getCode());
        }

        deleteTableDef(SCHEMA, "two");
        deleteTableDef(SCHEMA, "one");
    }

    @Test
    public void testDeleteDefinitionTwoTablesTwoVolumes() throws Exception {
        AkibanInformationSchema ais;

        createTableDef("drupal_a", "create table one (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `drupal_a`",
                   "create table `drupal_a`.one (id int, PRIMARY KEY (id)) engine=akibandb");
        ais = getAIS();
        assertNotNull(ais.getUserTable("drupal_a", "one"));

        createTableDef("drupal_b", "create table two (id int, PRIMARY KEY (id)) engine=akibandb;");
        assertDDLS("create schema if not exists `drupal_a`",
                   "create table `drupal_a`.one (id int, PRIMARY KEY (id)) engine=akibandb",
                   "create schema if not exists `drupal_b`",
                   "create table `drupal_b`.two (id int, PRIMARY KEY (id)) engine=akibandb");
        ais = getAIS();
        assertNotNull(ais.getUserTable("drupal_a", "one"));
        assertNotNull(ais.getUserTable("drupal_b", "two"));

        deleteTableDef("drupal_a", "one");
        assertDDLS("create schema if not exists `drupal_b`",
                   "create table `drupal_b`.two (id int, PRIMARY KEY (id)) engine=akibandb");
        ais = getAIS();
        assertNull(ais.getUserTable("drupal_a", "one"));
        assertNotNull(ais.getUserTable("drupal_b", "two"));
        
        deleteTableDef("drupal_b", "two");
        ais = getAIS();
        assertNull(ais.getUserTable("drupal_a", "two"));
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
        final AkibanInformationSchema ais = getAIS();
        final Session session = session();
        for (String name : tableNames) {
            final Table table = ais.getTable(schema, name);
            assertNotNull(schema + "." + name + " in AIS", table);
            final TableDefinition def = manager.getTableDefinition(session, schema, name);
            assertNotNull(schema + "." + name  + " has definition", def);
            expected.add(name);
        }
        final SortedSet<String> actual = new TreeSet<String>();
        for (TableDefinition def : manager.getTableDefinitions(session, schema).values()) {
            final Table table = ais.getTable(schema, def.getTableName());
            assertNotNull(def + " in AIS", table);
            actual.add(def.getTableName());
        }
        assertEquals("tables in: " + schema, expected, actual);
    }

    private void assertDDLS(String... statements) throws Exception{
        final List<String> actual = new ArrayList<String>();
        actual.addAll(AIS_CREATE_STATEMENTS);
        for(String s : statements) {
            actual.add(s + ';');
        }
        final List<String> expected = manager.schemaStrings(session(), false);
        assertEquals("DDLs", expected, actual);
    }
    
    private static List<String> readAisSchema() {
        final List<String> ddlList = new ArrayList<String>();
        ddlList.add("create schema if not exists `akiban_information_schema`;");
        final BufferedReader reader = new BufferedReader(new InputStreamReader(
                AkServer.class.getClassLoader()
                        .getResourceAsStream("akiban_information_schema.ddl")));
        for (String statement : (new MySqlStatementSplitter(reader))) {
            final String canonical = SchemaDef.canonicalStatement(statement);
            ddlList.add(canonical);
        }
        return ddlList;
    }
}
