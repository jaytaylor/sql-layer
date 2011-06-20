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

public class TableDDLTest {

    private TableName dropTable;
    private String    defaultSchema = "test";
    protected SQLParser parser;
    private DDLFunctionsMock ddlFunctions;
    private AkibanInformationSchema ais;
    
    @Before
    public void before() throws Exception {
        parser = new SQLParser();
        ddlFunctions = new DDLFunctionsMock();
    }
    
    @Test
    public void dropTableSimple() throws Exception {
        String sql = "DROP TABLE t1";
        
        dropTable = TableName.create(defaultSchema, "T1");
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropTableNode);
        
        TableDDL.dropTable(ddlFunctions, null, defaultSchema, (DropTableNode)stmt);
    }

    @Test
    public void dropTableSchema() throws Exception {
        String sql = "DROP TABLE foo.t1";
        
        dropTable = TableName.create("FOO", "T1");
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropTableNode);
        TableDDL.dropTable(ddlFunctions, null, defaultSchema, (DropTableNode)stmt);
    }
    
    @Test
    public void dropTableQuoted() throws Exception {
        String sql = "DROP TABLE \"t1\"";
        
        dropTable = TableName.create(defaultSchema, "t1");
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
    
    private class DDLFunctionsMock implements DDLFunctions {
        public DDLFunctionsMock() {}
        
        @Override
        public void createTable(Session session, UserTable table)
                throws UnsupportedCharsetException, ProtectedTableDDLException,
                DuplicateTableNameException, GroupWithProtectedTableException,
                JoinToUnknownTableException, JoinToWrongColumnsException,
                NoPrimaryKeyException, DuplicateColumnNameException,
                UnsupportedDataTypeException, JoinToMultipleParentsException,
                UnsupportedIndexDataTypeException,
                UnsupportedIndexSizeException, GenericInvalidOperationException {
            // TODO Auto-generated method stub
            
            assertEquals(table.getName(), dropTable);
            for (Column col : table.getColumnsIncludingInternal()) {
                assertNotNull (ais.getUserTable(dropTable).getColumn(col.getName()));
            }
            for (Column col : ais.getTable(dropTable).getColumnsIncludingInternal()) {
                assertNotNull (table.getColumn(col.getName()));
            }
            
            checkIndexes (table, ais.getUserTable(dropTable));
            checkIndexes (ais.getUserTable(dropTable), table);
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
        public void createIndexes(Session session,
                Collection<Index> indexesToAdd) throws NoSuchTableException,
                DuplicateKeyException, IndexAlterException,
                GenericInvalidOperationException {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void createTable(Session session, String schema, String ddlText)
                throws ParseException, UnsupportedCharsetException,
                ProtectedTableDDLException, DuplicateTableNameException,
                GroupWithProtectedTableException, JoinToUnknownTableException,
                JoinToWrongColumnsException, NoPrimaryKeyException,
                DuplicateColumnNameException, UnsupportedDataTypeException,
                JoinToMultipleParentsException,
                UnsupportedIndexDataTypeException,
                UnsupportedIndexSizeException, GenericInvalidOperationException {
            // TODO Auto-generated method stub
            
        }
        @Override
        public void dropGroup(Session session, String groupName)
                throws ProtectedTableDDLException,
                GenericInvalidOperationException {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void dropGroupIndexes(Session session, String groupName,
                Collection<String> indexesToDrop) throws NoSuchGroupException,
                IndexAlterException, GenericInvalidOperationException {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void dropSchema(Session session, String schemaName)
                throws ProtectedTableDDLException,
                ForeignConstraintDDLException, UnsupportedDropException,
                GenericInvalidOperationException {
            // TODO Auto-generated method stub
            
        }


        @Override
        public void dropTableIndexes(Session session, TableName tableName,
                Collection<String> indexesToDrop) throws NoSuchTableException,
                IndexAlterException, GenericInvalidOperationException {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void forceGenerationUpdate() {
            // TODO Auto-generated method stub
            
        }

        @Override
        public AkibanInformationSchema getAIS(Session session) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public List<String> getDDLs(Session session)
                throws InvalidOperationException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public int getGeneration() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public RowDef getRowDef(int tableId) throws NoSuchTableException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Table getTable(Session session, int tableId)
                throws NoSuchTableException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Table getTable(Session session, TableName tableName)
                throws NoSuchTableException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public int getTableId(Session session, TableName tableName)
                throws NoSuchTableException {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public TableName getTableName(Session session, int tableId)
                throws NoSuchTableException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public UserTable getUserTable(Session session, TableName tableName)
                throws NoSuchTableException {
            // TODO Auto-generated method stub
            return null;
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
    /*"CREATE TABLE t1 (c1 INT)";*/
    private void createTableSimpleGenerateAIS () {
        dropTable = TableName.create(defaultSchema, "T1");
        ais = new AkibanInformationSchema();
        AISBuilder builder = new AISBuilder(ais);
        
        builder.userTable(defaultSchema, "T1");
        builder.column(defaultSchema, "T1", "C1", 0, "int", Long.valueOf(0), Long.valueOf(0), true, false, null, null);
        builder.basicSchemaIsComplete();
    }
    
    /*CREATE TABLE t1 (c1 INT NOT NULL PRIMARY KEY)*/
    private void createTablePKGenerateAIS() {
        dropTable = TableName.create(defaultSchema, "T1");
        ais = new AkibanInformationSchema();
        AISBuilder builder = new AISBuilder(ais);
        
        builder.userTable(defaultSchema, "T1");
        builder.column(defaultSchema, "T1", "C1", 0, "int", (long)0, (long)0, false, false, null, null);
        builder.index(defaultSchema, "T1", "PRIMARY", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(defaultSchema, "T1", "PRIMARY", "C1", 0, true, 0);
        builder.basicSchemaIsComplete();
    }

    /*CREATE TABLE t1 (C1 int NOT NULL UNIQUE) */
    private void createTableUniqueKeyGenerateAIS() {
        dropTable = TableName.create(defaultSchema, "T1");
        ais = new AkibanInformationSchema();
        AISBuilder builder = new AISBuilder();
        
        builder.userTable(defaultSchema, "T1");
        builder.column(defaultSchema, "T1", "C1", 0, "int", (long)0, (long)0, false, false, null, null);
        builder.index(defaultSchema, "T1", "C1", true, Index.UNIQUE_KEY_CONSTRAINT);
        builder.indexColumn(defaultSchema, "T1", "C1", "C1", 0, true, 0);
    }
}
