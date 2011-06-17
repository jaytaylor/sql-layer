package com.akiban.sql.aisddl;

import java.util.Collection;
import java.util.List;

import org.junit.Before;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
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

public class TableDDLIT {

    
    @Before
    public void before() throws Exception {
        parser = new SQLParser();
    }
    

    protected SQLParser parser;

    private AkibanInformationSchema factory () throws Exception
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("s", "t");
        builder.column ("s", "t", "c1", 0, "int", null, null, false, false, null, null);
        builder.index("s", "t", "PRIMARY", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("s", "t", "PRIMARY", "c1", 0, true, 0);
        builder.basicSchemaIsComplete();
        return builder.akibanInformationSchema();
    }

    private class DDLFunctionsMock implements DDLFunctions {

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
        public void dropTable(Session session, TableName tableName)
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
}
