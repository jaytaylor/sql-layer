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

package com.foundationdb.sql.aisddl;

import com.foundationdb.server.api.ddl.DDLFunctionsMockBase;
import com.foundationdb.server.error.*;
import com.foundationdb.sql.StandardException;

import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.DropTableNode;
import com.foundationdb.sql.parser.CreateTableNode;
import com.foundationdb.server.types.service.TestTypesRegistry;
import com.foundationdb.server.types.service.TypesRegistry;

import java.util.List;
import java.util.Arrays;

public class TableDDLTest {

    private static TableName dropTable;
    private static final String    DEFAULT_SCHEMA = "test";
    private static final String    DEFAULT_TABLE  = "t1";
    private static final String    JOIN_TABLE = "t2";
    private static final String    JOIN_NAME = "test/t1/c1/test/test.t2/c2";
    protected SQLParser parser;
    private DDLFunctionsMock ddlFunctions;
    private TypesRegistry typesRegistry;
    private TestAISBuilder builder;

    @Before
    public void before() throws Exception {
        parser = new SQLParser();
        typesRegistry = TestTypesRegistry.MCOMPAT;
        builder = new TestAISBuilder(typesRegistry);
        ddlFunctions = new DDLFunctionsMock(builder.akibanInformationSchema());
    }
    
    @Test
    public void createNewTableWithIfNotExists() throws StandardException
    {
        String sql = "CREATE TABLE IF NOT EXISTS t1 (c1 INT)";
        createTableSimpleGenerateAIS();
        StatementNode createNode = parser.parseStatement(sql);
        assertTrue(createNode instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode) createNode, null);
    }

    @Test
    public void createDuplicateTableWithIfNotExists() throws StandardException
    {
        String sql = "CREATE TABLE IF NOT EXISTS " + DEFAULT_TABLE + "(c1 INT)";
        createTableSimpleGenerateAIS(); // creates DEFAULT_SCHEMA.DEFAULT_TABLE
        StatementNode createNode = parser.parseStatement(sql);
        assertTrue(createNode instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode) createNode, null);
    }

    @Test
    public void dropExistingTableWithIfExists() throws StandardException
    {
        String sql = "DROP TABLE IF EXISTS " + DEFAULT_TABLE;
        createTableSimpleGenerateAIS();
        StatementNode node = parser.parseStatement(sql);
        assertTrue(node instanceof DropTableNode);
        TableDDL.dropTable(ddlFunctions, null, DEFAULT_SCHEMA, (DropTableNode)node, null);
    }
    
    @Test
    public void dropNonExistingTableWithIfExists() throws StandardException
    {
        String sql = "DROP TABLE IF EXISTS chair";
        createTableSimpleGenerateAIS();
        StatementNode node = parser.parseStatement(sql);
        assertTrue(node instanceof DropTableNode);
        TableDDL.dropTable(ddlFunctions, null, DEFAULT_SCHEMA, (DropTableNode)node, null);
    }
    
    @Test
    public void dropTableSimple() throws Exception {
        String sql = "DROP TABLE t1";
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        createTableSimpleGenerateAIS ();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropTableNode);
        
        TableDDL.dropTable(ddlFunctions, null, DEFAULT_SCHEMA, (DropTableNode)stmt, null);
    }

    @Test
    public void dropTableSchemaTrue() throws Exception {
        String sql = "DROP TABLE test.t1";
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        createTableSimpleGenerateAIS ();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropTableNode);
        
        TableDDL.dropTable(ddlFunctions, null, DEFAULT_SCHEMA, (DropTableNode)stmt, null);
    }

    @Test (expected=NoSuchTableException.class)
    public void dropTableSchema() throws Exception {
        String sql = "DROP TABLE foo.t1";

        createTableSimpleGenerateAIS ();

        dropTable = TableName.create("foo", DEFAULT_TABLE);

        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropTableNode);
        TableDDL.dropTable(ddlFunctions, null, DEFAULT_SCHEMA, (DropTableNode)stmt, null);
    }
    
    @Test (expected=NoSuchTableException.class)
    public void dropTableQuoted() throws Exception {
        String sql = "DROP TABLE \"T1\"";

        dropTable = TableName.create(DEFAULT_SCHEMA, "T1");

        createTableSimpleGenerateAIS ();

        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropTableNode);
        TableDDL.dropTable(ddlFunctions, null, DEFAULT_SCHEMA, (DropTableNode)stmt, null);
    }

    @Test
    public void createTableSimple() throws Exception {
        makeSeparateAIS();
        String sql = "CREATE TABLE t1 (c1 INT)";
        createTableSimpleGenerateAIS();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null);

    }
    
    @Test
    public void createTablePK() throws Exception {
        makeSeparateAIS();
        String sql = "CREATE TABLE t1 (c1 INT NOT NULL PRIMARY KEY)";
        createTablePKGenerateAIS();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null);
    }
    
    @Test
    public void createTableUniqueKey() throws Exception {
        makeSeparateAIS();
        String sql = "CREATE TABLE t1 (C1 int NOT NULL UNIQUE)";
        createTableUniqueKeyGenerateAIS();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null);
    }

    @Test (expected=DuplicateIndexException.class)
    public void createTable2PKs() throws Exception {
        String sql = "CREATE TABLE test.t1 (c1 int primary key, c2 int NOT NULL, primary key (c2))";
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null);
    }
    
    @Test
    public void createTableFKSimple() throws Exception {
        makeSeparateAIS();
        String sql = "CREATE TABLE t2 (c1 int not null primary key, c2 int not null, grouping foreign key (c2) references t1)";
        createTableFKSimpleGenerateAIS();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null);
    }

    @Test
    public void createTableAs1() throws Exception {
        makeSeparateAIS();
        String sql = "CREATE TABLE t1 (column1, column2, column3) AS (SELECT c1, c2, c3 FROM t2) WITH DATA";
        createTableAsColumnGenerateAIS();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        List<String> columnNames = Arrays.asList("c1", "c2", "c3");
        DataTypeDescriptor d = new DataTypeDescriptor(TypeId.INTEGER_ID, false);
        List<DataTypeDescriptor> descriptors = Arrays.asList(d,d,d);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null, descriptors ,columnNames);
    }

    @Test
    public void createTableAs2() throws Exception {
        makeSeparateAIS();
        String sql = "CREATE TABLE t1 (column1, column2, column3) AS (SELECT * FROM t2) WITH DATA";
        createTableAsColumnGenerateAIS();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        List<String> columnNames = Arrays.asList("c1", "c2", "c3");
        DataTypeDescriptor d = new DataTypeDescriptor(TypeId.INTEGER_ID, false);
        List<DataTypeDescriptor> descriptors = Arrays.asList(d,d,d);

        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null, descriptors ,columnNames);
    }

    @Test
    public void createTableAs3() throws Exception {
        makeSeparateAIS();
        String sql = "CREATE TABLE t1 AS (SELECT c1, c2, c3 FROM t2) WITH DATA";
        createTableAsCGenerateAIS();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        List<String> columnNames = Arrays.asList("c1", "c2", "c3");
        DataTypeDescriptor d = new DataTypeDescriptor(TypeId.INTEGER_ID, false);
        List<DataTypeDescriptor> descriptors = Arrays.asList(d,d,d);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null, descriptors ,columnNames);
    }

    @Test
    public void createTableAs4() throws Exception {
        makeSeparateAIS();
        String sql = "CREATE TABLE t1 AS (SELECT * FROM t2) WITH DATA";
        createTableAsCGenerateAIS();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        List<String> columnNames = Arrays.asList("c1", "c2", "c3");
        DataTypeDescriptor d = new DataTypeDescriptor(TypeId.INTEGER_ID, false);
        List<DataTypeDescriptor> descriptors = Arrays.asList(d,d,d);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null, descriptors ,columnNames);
    }

    @Test
    public void createTableAs5() throws Exception {
        makeSeparateAIS();
        String sql = "CREATE TABLE t1 (c1, c2) AS (SELECT column1, column2, column3 FROM t2) WITH DATA";
        createTableAsMixGenerateAIS();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        List<String> columnNames = Arrays.asList("column1", "column2", "column3");
        DataTypeDescriptor d = new DataTypeDescriptor(TypeId.INTEGER_ID, false);
        List<DataTypeDescriptor> descriptors = Arrays.asList(d,d,d);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null, descriptors ,columnNames);
    }

    @Test (expected=InvalidCreateAsException.class)
    public void createTableAs6() throws Exception {
        makeSeparateAIS();
        String sql = "CREATE TABLE t1 (c1, c2, c3) AS (SELECT column1, column2 FROM t2) WITH DATA";
        createTableAsMixGenerateAIS();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        List<String> columnNames = Arrays.asList("column1", "column2");
        DataTypeDescriptor d = new DataTypeDescriptor(TypeId.INTEGER_ID, false);
        List<DataTypeDescriptor> descriptors = Arrays.asList(d,d);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null, descriptors ,columnNames);
    }

    @Test
    public void columnDefaultsArePreserved() throws StandardException {
        final String DEFAULT_C1 = "50";
        final String DEFAULT_C2 = "ban ana";
        makeSeparateAIS();
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.table(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", 0, "MCOMPAT", "int", null, null, true, DEFAULT_C1, null);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c2", 1, "MCOMPAT", "varchar", 32L, null, true, DEFAULT_C2, null);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();

        String sql = String.format("CREATE TABLE t1 (c1 INT DEFAULT %s, c2 VARCHAR(32) DEFAULT '%s')",
                                   DEFAULT_C1, DEFAULT_C2);
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null);
    }

    @Test
    public void columnGeneratedByDefaultHasNoDefaultValue() throws StandardException {
        makeSeparateAIS();
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.table(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", 0, "MCOMPAT", "int", false);
        builder.sequence(DEFAULT_SCHEMA, "sequence_c1", 1, 1, 0, 1000, false);
        builder.columnAsIdentity(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", "sequence_c1", true);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();

        String sql = "CREATE TABLE t1 (c1 INT GENERATED BY DEFAULT AS IDENTITY)";
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null);
    }

    @Test
    public void columnGeneratedAlwaysHasNoDefaultValue() throws StandardException {
        makeSeparateAIS();
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.table(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", 0, "MCOMPAT", "int", false);
        builder.sequence(DEFAULT_SCHEMA, "sequence_c1", 1, 1, 0, 1000, false);
        builder.columnAsIdentity(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", "sequence_c1", false);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();

        String sql = "CREATE TABLE t1 (c1 INT GENERATED ALWAYS AS IDENTITY)";
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null);
    }
    
    @Test
    public void columnSerial() throws StandardException {
        makeSeparateAIS();
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.table(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", 0, "MCOMPAT", "int", false, true);
        builder.sequence(DEFAULT_SCHEMA, "sequence_c1", 1, 1, 0, 1000, false);
        builder.columnAsIdentity(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", "sequence_c1", true);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();

        String sql = "Create Table " + DEFAULT_TABLE + " (c1 SERIAL)";
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null);
    }

    @Test
    public void columnBigSerial() throws StandardException {
        makeSeparateAIS();
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.table(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", 0, "MCOMPAT", "bigint", false, true);
        builder.sequence(DEFAULT_SCHEMA, "sequence_c1", 1, 1, 0, 1000, false);
        builder.columnAsIdentity(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", "sequence_c1", true);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();

        String sql = "Create Table " + DEFAULT_TABLE + " (c1 BIGSERIAL)";
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null);
    }
    
    //bug1047037
    @Test (expected=DuplicateIndexException.class)
    public void namedPKConstraint() throws StandardException {
        String sql = "CREATE TABLE t1 (c1 INT NOT NULL PRIMARY KEY, CONSTRAINT co1 PRIMARY KEY (c1))";
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue(stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null);
    }

    @Test
    public void contraintPKAdjustedNotNull() throws StandardException {
        String sql = "CREATE TABLE t1 (c1 int PRIMARY KEY)";

        makeSeparateAIS();
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.table(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", 0, "MCOMPAT", "int", false, true);
        builder.pk(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.indexColumn(DEFAULT_SCHEMA, DEFAULT_TABLE, "PRIMARY", "c1", 0, true, 0);

        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue(stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null);
    }
    
    @Test
    public void contraintPKAdjustedNotNullSeparate() throws StandardException {
        String sql = "CREATE TABLE t1 (c1 int, PRIMARY KEY(c1))";

        makeSeparateAIS();
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.table(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", 0, "MCOMPAT", "int", false, true);
        builder.pk(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.indexColumn(DEFAULT_SCHEMA, DEFAULT_TABLE, "PRIMARY", "c1", 0, true, 0);

        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue(stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null);
    }
    
    @Test
    public void contraintPKAdjustedNotNullFirst() throws StandardException {
        String sql = "CREATE TABLE t1 (c1 int, c2 varchar(32) not null, PRIMARY KEY(c1,c2))";

        makeSeparateAIS();
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.table(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", 0, "MCOMPAT", "int", false, false);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c2", 1, "MCOMPAT", "varchar", 32L, 0L, false);
        builder.pk(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.indexColumn(DEFAULT_SCHEMA, DEFAULT_TABLE, "PRIMARY", "c1", 0, true, 0);
        builder.indexColumn(DEFAULT_SCHEMA, DEFAULT_TABLE, "PRIMARY", "c2", 1, true, 0);

        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue(stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null);
    }

    @Test
    public void contraintPKAdjustedNotNullSecond() throws StandardException {
        String sql = "CREATE TABLE t1 (c1 int NOT NULL, c2 varchar(32), PRIMARY KEY(c1,c2))";

        makeSeparateAIS();
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.table(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", 0, "MCOMPAT", "int", false, false);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c2", 1, "MCOMPAT", "varchar", 32L, 0L, false);
        builder.pk(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.indexColumn(DEFAULT_SCHEMA, DEFAULT_TABLE, "PRIMARY", "c1", 0, true, 0);
        builder.indexColumn(DEFAULT_SCHEMA, DEFAULT_TABLE, "PRIMARY", "c2", 1, true, 0);

        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue(stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null);
    }

    @Test
    public void contraintPKAdjustedNotNullBoth() throws StandardException {
        String sql = "CREATE TABLE t1 (c1 int, c2 varchar(32), PRIMARY KEY(c1,c2))";

        makeSeparateAIS();
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.table(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", 0, "MCOMPAT", "int", false, false);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c2", 1, "MCOMPAT", "varchar", 32L, 0L, false);
        builder.pk(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.indexColumn(DEFAULT_SCHEMA, DEFAULT_TABLE, "PRIMARY", "c1", 0, true, 0);
        builder.indexColumn(DEFAULT_SCHEMA, DEFAULT_TABLE, "PRIMARY", "c2", 1, true, 0);

        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue(stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, DEFAULT_SCHEMA, (CreateTableNode)stmt, null);
    }

    
    public static class DDLFunctionsMock extends DDLFunctionsMockBase {
        private final AkibanInformationSchema internalAIS;
        private final AkibanInformationSchema externalAIS;

        public DDLFunctionsMock(AkibanInformationSchema ais) {
            this.internalAIS = ais;
            this.externalAIS = ais;
        }

        public DDLFunctionsMock(AkibanInformationSchema internal, AkibanInformationSchema external) {
            this.internalAIS = internal;
            this.externalAIS = external;
        }

        private static void checkColumn(Column expected, Column actual) {
            assertNotNull("actual column name", actual.getName());
            assertNotNull("expected column", expected);
            assertEquals("type", expected.getType().typeClass(), actual.getType().typeClass());
            assertEquals("is nullable", expected.getNullable(), actual.getNullable());
            assertEquals("default value", expected.getDefaultValue(), actual.getDefaultValue());
            assertEquals("identity", expected.getIdentityGenerator() != null, actual.getIdentityGenerator() != null);
        }

        @Override
        public void createTable(Session session, Table table) {

            assertEquals(table.getName(), dropTable);

            final Table dropAisTable = internalAIS.getTable(dropTable);
            assertNotNull("expected table", dropAisTable);
            for (Column col : table.getColumnsIncludingInternal()) {
                checkColumn(dropAisTable.getColumn(col.getName()), col);
            }
            for (Column col : internalAIS.getTable(dropTable).getColumnsIncludingInternal()) {
                checkColumn(dropAisTable.getColumn(col.getName()), col);
            }
            
            checkIndexes (table, dropAisTable);
            checkIndexes (dropAisTable, table);
            
            if (table.getParentJoin() != null) {
                checkJoin (table.getParentJoin(), internalAIS.getJoin(JOIN_NAME));
            }
        }

        private void checkIndexes(Table sourceTable, Table checkTable) {
            for (Index index : sourceTable.getIndexesIncludingInternal()) {
                assertNotNull(checkTable.getIndexIncludingInternal(index.getIndexName().getName()));
                Index checkIndex = checkTable.getIndexIncludingInternal(index.getIndexName().getName());
                for (IndexColumn col : index.getKeyColumns()) {
                    checkIndex.getKeyColumns().get(col.getPosition());
                }
            }
        }

        private void checkJoin (Join sourceJoin, Join checkJoin) {
            assertEquals (sourceJoin.getName(), checkJoin.getName()); 
            assertEquals (sourceJoin.getJoinColumns().size(), checkJoin.getJoinColumns().size());
            for (int i = 0; i < sourceJoin.getJoinColumns().size(); i++) {
                JoinColumn sourceColumn = sourceJoin.getJoinColumns().get(i);
                JoinColumn checkColumn = checkJoin.getJoinColumns().get(i);
                
                assertEquals (sourceColumn.getChild().getName(), checkColumn.getChild().getName());
                assertEquals (sourceColumn.getParent().getName(), checkColumn.getParent().getName());
            }
        }

        @Override
        public void dropTable(Session session, TableName tableName) {
            assertEquals(tableName, dropTable);
        }

        @Override
        public AkibanInformationSchema getAIS(Session session) {
            return externalAIS;
        }
    } // END class DDLFunctionsMock

    private void makeSeparateAIS() {
        AkibanInformationSchema external = new AkibanInformationSchema();
        ddlFunctions = new DDLFunctionsMock(builder.akibanInformationSchema(), external);
    }

    /*"CREATE TABLE t1 (c1 INT)";*/
    private void createTableSimpleGenerateAIS () {
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        
        builder.table(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", 0, "MCOMPAT", "int", true);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
    }
    
    /*CREATE TABLE t1 (c1 INT NOT NULL PRIMARY KEY)*/
    private void createTablePKGenerateAIS() {
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        
        builder.table(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", 0, "MCOMPAT", "int", false);
        builder.pk(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.indexColumn(DEFAULT_SCHEMA, DEFAULT_TABLE, "PRIMARY", "c1", 0, true, 0);
        builder.basicSchemaIsComplete();
    }

    /*CREATE TABLE t1 (C1 int NOT NULL UNIQUE) */
    private void createTableUniqueKeyGenerateAIS() {
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        
        builder.table(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", 0, "MCOMPAT", "int", false);
        builder.unique(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1");
        builder.indexColumn(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", "c1", 0, true, 0);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
    }

    /* CREATE TABLE t1 (c1 int not null primary key) */
    /* CREATE TABLE t2 (c1 int not null primary key, c2 int not null, grouping foreign key (c2) references t1) */
    private void createTableFKSimpleGenerateAIS() {
        dropTable = TableName.create(DEFAULT_SCHEMA, JOIN_TABLE);

        TestAISBuilder builders[] = { builder, new TestAISBuilder(ddlFunctions.externalAIS, typesRegistry) };

        // Re-gen the DDLFunctions to have the AIS for internal references. 
        ddlFunctions = new DDLFunctionsMock(builder.akibanInformationSchema(), ddlFunctions.externalAIS);
        // Need t1 in both internal and external
        for(TestAISBuilder b : builders) {
            // table t1:
            b.table(DEFAULT_SCHEMA, DEFAULT_TABLE);
            b.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", 0, "MCOMPAT", "int", false);
            b.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c2", 1, "MCOMPAT", "int", false);
            b.pk(DEFAULT_SCHEMA, DEFAULT_TABLE);
            b.indexColumn(DEFAULT_SCHEMA, DEFAULT_TABLE, Index.PRIMARY, "c1", 0, true, 0);
            b.basicSchemaIsComplete();
            b.createGroup("t1", DEFAULT_SCHEMA);
            b.addTableToGroup("t1", DEFAULT_SCHEMA, DEFAULT_TABLE);
            b.groupingIsComplete();
        }
        
        // table t2:
        builder.table(DEFAULT_SCHEMA, JOIN_TABLE);
        builder.column(DEFAULT_SCHEMA, JOIN_TABLE, "c1", 0, "MCOMPAT", "int", false);
        builder.column(DEFAULT_SCHEMA, JOIN_TABLE, "c2", 1, "MCOMPAT", "int", false);
        builder.pk(DEFAULT_SCHEMA, JOIN_TABLE);
        builder.indexColumn(DEFAULT_SCHEMA, JOIN_TABLE, "PRIMARY", "c1", 0, true, 0);
        builder.basicSchemaIsComplete();
        // do the join
        builder.joinTables(JOIN_NAME, DEFAULT_SCHEMA, DEFAULT_TABLE, DEFAULT_SCHEMA, JOIN_TABLE);
        builder.joinColumns(JOIN_NAME, DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", DEFAULT_SCHEMA, JOIN_TABLE, "c2");
        
        builder.addJoinToGroup("t1", JOIN_NAME, 0);
        builder.groupingIsComplete();
    }

    private void createTableAsCGenerateAIS () {
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.table(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", 0, "MCOMPAT", "int", false);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c2", 1, "MCOMPAT", "int", false);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c3", 2, "MCOMPAT", "int", false);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
    }

    private void createTableAsColumnGenerateAIS () {
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.table(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "column1", 0, "MCOMPAT", "int", false);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "column2", 1, "MCOMPAT", "int", false);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "column3", 2, "MCOMPAT", "int", false);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
    }

    private void createTableAsMixGenerateAIS () {
        dropTable = TableName.create(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.table(DEFAULT_SCHEMA, DEFAULT_TABLE);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c1", 0, "MCOMPAT", "int", false);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "c2", 1, "MCOMPAT", "int", false);
        builder.column(DEFAULT_SCHEMA, DEFAULT_TABLE, "column3", 2, "MCOMPAT", "int", false);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
    }
}
