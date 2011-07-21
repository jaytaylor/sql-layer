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
package com.akiban.sql.aisddl;

import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowDef;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.GenericInvalidOperationException;
import com.akiban.server.api.common.NoSuchGroupException;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.api.ddl.DuplicateColumnNameException;
import com.akiban.server.api.ddl.DuplicateTableNameException;
import com.akiban.server.api.ddl.ForeignConstraintDDLException;
import com.akiban.server.api.ddl.GroupWithProtectedTableException;
import com.akiban.server.api.ddl.IndexAlterException;
import com.akiban.server.api.ddl.JoinToMultipleParentsException;
import com.akiban.server.api.ddl.JoinToUnknownTableException;
import com.akiban.server.api.ddl.JoinToWrongColumnsException;
import com.akiban.server.api.ddl.NoPrimaryKeyException;
import com.akiban.server.api.ddl.ParseException;
import com.akiban.server.api.ddl.ProtectedTableDDLException;
import com.akiban.server.api.ddl.UnsupportedCharsetException;
import com.akiban.server.api.ddl.UnsupportedDataTypeException;
import com.akiban.server.api.ddl.UnsupportedDropException;
import com.akiban.server.api.ddl.UnsupportedIndexDataTypeException;
import com.akiban.server.api.ddl.UnsupportedIndexSizeException;
import com.akiban.server.api.dml.DuplicateKeyException;
import com.akiban.server.service.session.Session;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.DropTableNode;
import com.akiban.sql.parser.CreateTableNode;
import com.akiban.sql.StandardException;

public class TableDDLTest {

    private static TableName dropTable;
    private String    defaultSchema = "test";
    private String    defaultTable  = "t1";
    private String    joinTable = "t2";
    protected SQLParser parser;
    private DDLFunctionsMock ddlFunctions;
    private static AkibanInformationSchema ais;
    private AISBuilder builder;
    @Before
    public void before() throws Exception {
        parser = new SQLParser();
        ddlFunctions = new DDLFunctionsMock();
        ais = new AkibanInformationSchema();
        builder = new AISBuilder(ais);
    }
    
    @Test
    public void dropTableSimple() throws Exception {
        String sql = "DROP TABLE t1";
        
        dropTable = TableName.create(defaultSchema, defaultTable);
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropTableNode);
        
        TableDDL.dropTable(ddlFunctions, null, defaultSchema, (DropTableNode)stmt);
    }

    @Test
    public void dropTableSchema() throws Exception {
        String sql = "DROP TABLE foo.t1";
        
        dropTable = TableName.create("foo", defaultTable);
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropTableNode);
        TableDDL.dropTable(ddlFunctions, null, defaultSchema, (DropTableNode)stmt);
    }
    
    @Test
    public void dropTableQuoted() throws Exception {
        String sql = "DROP TABLE \"T1\"";
        
        dropTable = TableName.create(defaultSchema, "T1");
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropTableNode);
        TableDDL.dropTable(ddlFunctions, null, defaultSchema, (DropTableNode)stmt);
    }
    /*
    create table t1 (col1 int primary key)
    CREATE TABLE "MyMixedCaseTable" ("Col1" int primary key)
    create table test.t1 (col1 integer, primary key (col1))
    CREATE TABLE t1 (col1 int primary key, foreign key (col1) references t2 (col2))
    create table t1 (col1 integer, 
    primary key (col1), 
    grouping foreign key (col1) references t2 (col1))
    */    
    @Test
    public void createTableSimple() throws Exception {
        String sql = "CREATE TABLE t1 (c1 INT)";
        
        createTableSimpleGenerateAIS();

        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, defaultSchema, (CreateTableNode)stmt);
        
    }
    
    @Test
    public void createTablePK() throws Exception {
        String sql = "CREATE TABLE t1 (c1 INT NOT NULL PRIMARY KEY)";

        createTablePKGenerateAIS();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, defaultSchema, (CreateTableNode)stmt);
    }

    @Test
    public void createTableUniqueKey() throws Exception {
        String sql = "CREATE TABLE t1 (C1 int NOT NULL UNIQUE)";
        createTableUniqueKeyGenerateAIS();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, defaultSchema, (CreateTableNode)stmt);
    }

    @Test (expected=StandardException.class)
    public void createTable2PKs() throws Exception {
        String sql = "CREATE TABLE test.t1 (c1 int primary key, c2 int NOT NULL, primary key (c2))";
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, defaultSchema, (CreateTableNode)stmt);
    }
    
    @Test
    public void createTableFKSimple() throws Exception {
        String sql = "CREATE TABLE t2 (c1 int primary key, c2 int not null, grouping foreign key (c2) references t1)";
        createTableFKSimpleGenerateAIS();
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateTableNode);
        TableDDL.createTable(ddlFunctions, null, defaultSchema, (CreateTableNode)stmt);
    }
    
    public static class DDLFunctionsMock implements DDLFunctions {
        private AkibanInformationSchema internalAIS = null;
        public DDLFunctionsMock() {}
        
        public DDLFunctionsMock(AkibanInformationSchema ais) { this.internalAIS = ais; }
        
        @Override
        public void createTable(Session session, UserTable table)
                throws UnsupportedCharsetException, ProtectedTableDDLException,
                DuplicateTableNameException, GroupWithProtectedTableException,
                JoinToUnknownTableException, JoinToWrongColumnsException,
                NoPrimaryKeyException, DuplicateColumnNameException,
                UnsupportedDataTypeException, JoinToMultipleParentsException,
                UnsupportedIndexDataTypeException,
                UnsupportedIndexSizeException, GenericInvalidOperationException {
            
            assertEquals(table.getName(), dropTable);
            for (Column col : table.getColumnsIncludingInternal()) {
                assertNotNull (col.getName());
                assertNotNull (ais.getUserTable(dropTable));
                assertNotNull (ais.getUserTable(dropTable).getColumn(col.getName()));
            }
            for (Column col : ais.getTable(dropTable).getColumnsIncludingInternal()) {
                assertNotNull (col.getName());
                assertNotNull (table.getColumn(col.getName()));
            }
            
            checkIndexes (table, ais.getUserTable(dropTable));
            checkIndexes (ais.getUserTable(dropTable), table);
            
            if (table.getParentJoin() != null) {
                checkJoin (table.getParentJoin(), ais.getJoin("t1"));
            }
            
        }

        private void checkIndexes(UserTable sourceTable, UserTable checkTable) {
            for (Index index : sourceTable.getIndexesIncludingInternal()) {
                assertNotNull(checkTable.getIndexIncludingInternal(index.getIndexName().getName()));
                Index checkIndex = checkTable.getIndexIncludingInternal(index.getIndexName().getName());
                for (IndexColumn col : index.getColumns()) {
                    checkIndex.getColumns().get(col.getPosition());
                }
            }
        }
        private void checkJoin (Join sourceJoin, Join checkJoin) {
            assertEquals (sourceJoin.getName(), checkJoin.getName()); 
            
            //TODO : check what?
        }

        @Override
        public void dropTable(Session session, TableName tableName)
                throws ProtectedTableDDLException,
                ForeignConstraintDDLException, UnsupportedDropException,
                GenericInvalidOperationException {
            // TODO Auto-generated method stub
            assertEquals(tableName, dropTable);
        }
        @Override
        public AkibanInformationSchema getAIS(Session session) {
            return internalAIS;
        }

        @Override
        public void createIndexes(Session session,
                Collection<Index> indexesToAdd) throws NoSuchTableException,
                DuplicateKeyException, IndexAlterException,
                GenericInvalidOperationException {}

        @Override
        public void createTable(Session session, String schema, String ddlText)
                throws ParseException, UnsupportedCharsetException,
                ProtectedTableDDLException, DuplicateTableNameException,
                GroupWithProtectedTableException, JoinToUnknownTableException,
                JoinToWrongColumnsException, NoPrimaryKeyException,
                DuplicateColumnNameException, UnsupportedDataTypeException,
                JoinToMultipleParentsException,
                UnsupportedIndexDataTypeException,
                UnsupportedIndexSizeException, GenericInvalidOperationException {}
        @Override
        public void dropGroup(Session session, String groupName)
                throws ProtectedTableDDLException,
                GenericInvalidOperationException {}

        @Override
        public void dropGroupIndexes(Session session, String groupName,
                Collection<String> indexesToDrop) throws NoSuchGroupException,
                IndexAlterException, GenericInvalidOperationException {}

        @Override
        public void dropSchema(Session session, String schemaName)
                throws ProtectedTableDDLException,
                ForeignConstraintDDLException, UnsupportedDropException,
                GenericInvalidOperationException {}

        @Override
        public void dropTableIndexes(Session session, TableName tableName,
                Collection<String> indexesToDrop) throws NoSuchTableException,
                IndexAlterException, GenericInvalidOperationException {}

        @Override
        public void forceGenerationUpdate() {}


        @Override
        public List<String> getDDLs(Session session)
                throws InvalidOperationException {
            return null;
        }

        @Override
        public int getGeneration() {
            return 0;
        }

        @Override
        public RowDef getRowDef(int tableId) throws NoSuchTableException {
            return null;
        }

        @Override
        public Table getTable(Session session, int tableId)
                throws NoSuchTableException {
            return null;
        }

        @Override
        public Table getTable(Session session, TableName tableName)
                throws NoSuchTableException {
            return null;
        }

        @Override
        public int getTableId(Session session, TableName tableName)
                throws NoSuchTableException {
            return 0;
        }

        @Override
        public TableName getTableName(Session session, int tableId)
                throws NoSuchTableException {
            return null;
        }

        @Override
        public UserTable getUserTable(Session session, TableName tableName)
                throws NoSuchTableException {
            return null;
        }
    } // END class DDLFunctionsMock

    /*"CREATE TABLE t1 (c1 INT)";*/
    private void createTableSimpleGenerateAIS () {
        dropTable = TableName.create(defaultSchema, defaultTable);
        
        builder.userTable(defaultSchema, defaultTable);
        builder.column(defaultSchema, defaultTable, "c1", 0, "int", Long.valueOf(0), Long.valueOf(0), true, false, null, null);
        builder.basicSchemaIsComplete();
    }
    
    /*CREATE TABLE t1 (c1 INT NOT NULL PRIMARY KEY)*/
    private void createTablePKGenerateAIS() {
        dropTable = TableName.create(defaultSchema, defaultTable);
        
        builder.userTable(defaultSchema, defaultTable);
        builder.column(defaultSchema, defaultTable, "c1", 0, "int", (long)0, (long)0, false, false, null, null);
        builder.index(defaultSchema, defaultTable, "PRIMARY", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(defaultSchema, defaultTable, "PRIMARY", "c1", 0, true, 0);
        builder.basicSchemaIsComplete();
    }

    /*CREATE TABLE t1 (C1 int NOT NULL UNIQUE) */
    private void createTableUniqueKeyGenerateAIS() {
        dropTable = TableName.create(defaultSchema, defaultTable);
        
        builder.userTable(defaultSchema, defaultTable);
        builder.column(defaultSchema, defaultTable, "c1", 0, "int", (long)0, (long)0, false, false, null, null);
        builder.index(defaultSchema, defaultTable, "c1", true, Index.UNIQUE_KEY_CONSTRAINT);
        builder.indexColumn(defaultSchema, defaultTable, "c1", "c1", 0, true, 0);
        builder.basicSchemaIsComplete();
    }
    
    private void createTableFKSimpleGenerateAIS() {
        dropTable = TableName.create(defaultSchema, joinTable);

        // table t1:
        builder.userTable(defaultSchema, defaultTable);
        builder.column(defaultSchema, defaultTable, "c1", 0, "int", (long)0, (long)0, false, false, null, null);
        builder.column(defaultSchema, defaultTable, "c2", 1, "int", (long)0, (long)0, false, false, null, null);
        builder.index(defaultSchema, defaultTable, "pk", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(defaultSchema, defaultTable, "pk", "c1", 0, true, 0);
        // table t2:
        builder.userTable(defaultSchema, joinTable);
        builder.column(defaultSchema, joinTable, "c1", 0, "int", (long)0, (long)0, false, false, null, null);
        builder.column(defaultSchema, joinTable, "c2", 1, "int", (long)0, (long)0, false, false, null, null);
        builder.index(defaultSchema, joinTable, "PRIMARY", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(defaultSchema, joinTable, "PRIMARY", "c1", 0, true, 0);
        builder.basicSchemaIsComplete();
        // group
        builder.createGroup("t1", defaultSchema, "_akiban_t1");
        builder.addTableToGroup("t1", defaultSchema, defaultTable);
        // do the join
        builder.joinTables("test/t1/test/t2", defaultSchema, defaultTable, defaultSchema, joinTable);
        builder.joinColumns("test/t1/test/t2", defaultSchema, defaultTable, "c1", defaultSchema, joinTable, "c1");
        
        builder.addJoinToGroup("t1", "test/t1/test/t2", 0);
        builder.groupingIsComplete();
    }
}
