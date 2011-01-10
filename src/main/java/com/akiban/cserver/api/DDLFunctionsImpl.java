package com.akiban.cserver.api;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.ddl.DuplicateColumnNameException;
import com.akiban.cserver.api.ddl.DuplicateTableNameException;
import com.akiban.cserver.api.ddl.ForeignConstraintDDLException;
import com.akiban.cserver.api.ddl.GroupWithProtectedTableException;
import com.akiban.cserver.api.ddl.JoinToUnknownTableException;
import com.akiban.cserver.api.ddl.JoinToWrongColumnsException;
import com.akiban.cserver.api.ddl.NoPrimaryKeyException;
import com.akiban.cserver.api.ddl.ParseException;
import com.akiban.cserver.api.ddl.ProtectedTableDDLException;
import com.akiban.cserver.api.ddl.UnsupportedCharsetException;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.store.SchemaId;
import com.akiban.message.ErrorCode;

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map.Entry;

public final class DDLFunctionsImpl extends ClientAPIBase implements
        DDLFunctions {

    public static DDLFunctions instance() {
        return new DDLFunctionsImpl();
    }

    @Override
    public void createTable(Session session, String schema, String ddlText)
            throws ParseException, UnsupportedCharsetException,
            ProtectedTableDDLException, DuplicateTableNameException,
            GroupWithProtectedTableException, JoinToUnknownTableException,
            JoinToWrongColumnsException, NoPrimaryKeyException,
            DuplicateColumnNameException, GenericInvalidOperationException {
        try {
            schemaManager().createTableDefinition(session, schema, ddlText);
        } catch (Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }

    @Override
    public void dropTable(Session session, TableId tableId)
            throws ProtectedTableDDLException, ForeignConstraintDDLException,
            GenericInvalidOperationException {
        final TableName tableName;
        final int rowDefId;
        try {
            tableName = tableId.getTableName(idResolver());
            rowDefId = tableId.getTableId(idResolver());
        } catch (NoSuchTableException e) {
            return; // dropping a nonexistent table is a no-op
        }

        try {
            // TODO - reconsider the API for truncateTable.
            // TODO - needs to be wrapped in a Transaction
            store().truncateTable(session, rowDefId);
            schemaManager().deleteTableDefinition(session, tableName.getSchemaName(),
                    tableName.getTableName());
        } catch (Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }

    @Override
    public void dropSchema(Session session, String schemaName)
            throws ProtectedTableDDLException, ForeignConstraintDDLException,
            GenericInvalidOperationException {
        try {
            schemaManager().deleteSchemaDefinition(session, schemaName);
        } catch (Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }

    @Override
    public AkibaInformationSchema getAIS(final Session session) {
        return schemaManager().getAis(session);
    }

    @Override
    public TableName getTableName(TableId tableId) throws NoSuchTableException {
        return tableId.getTableName(idResolver());
    }

    @Override
    public TableId resolveTableId(TableId tableId) throws NoSuchTableException {
        tableId.getTableId(idResolver());
        tableId.getTableName(idResolver());
        return tableId;
    }

    @Override
    public String getDDLs(final Session session) throws InvalidOperationException {
        try {
            // TODO - note: the boolean value determines whether the text
            // of CREATE TABLE statements for group tables will be generated.
            // Since Halo won't be used for queries, I'm setting this to false
            // for now. -- Peter
            return schemaManager().schemaString(
                    session, false);
        } catch (Exception e) {
            throw new InvalidOperationException(ErrorCode.UNEXPECTED_EXCEPTION,
                    "Unexpected exception", e);
        }
    }

    @Override
    public SchemaId getSchemaID() throws InvalidOperationException {
        return new SchemaId(schemaManager().getSchemaGeneration());
    }

    @Override
    @SuppressWarnings("unused")
    // meant to be used from JMX
    public void forceGenerationUpdate() throws InvalidOperationException {
        schemaManager().forceNewTimestamp();
    }

    @Override
    public void createIndexes(final Session session, AkibaInformationSchema ais) throws InvalidOperationException {
        try {
            if(ais.getUserTables().size() != 1) {
                throw new Exception("Too many user tables");
            }
            
            AkibaInformationSchema cur_ais = getAIS(session);
            Entry<TableName, UserTable> newIndexesEntry = ais.getUserTables().entrySet().iterator().next();
            UserTable cur_utable = cur_ais.getUserTable(newIndexesEntry.getKey());
            
            if(cur_utable == null) {
                throw new Exception("Uknown table");
            }
            
            int max_id = 0;
            Set<IndexName> cur_names = new HashSet<IndexName>();
            for(Index i : cur_utable.getIndexes()) {
                max_id = Math.max(max_id, i.getIndexId().intValue());
                cur_names.add(i.getIndexName());
            }
            
            for(Index i: newIndexesEntry.getValue().getIndexes()) {
                // AIS Reader.close() adds a pkey to all UserTables that get instantiated.
                // This interface does not do pkey additions so skip it.
                if(i.isPrimaryKey()) continue;
                
                if(cur_names.contains(i.getIndexName())) {
                    throw new Exception("Duplicate index name");
                }
                
                i.setIndexId(++max_id);
                System.out.println(String.format("DDLFunctionsImpl.createIndexes: %s:%d", i.getIndexName().getName(), i.getIndexId()));
            }
            
            // All were valid, add to current AIS
            for(Index i: newIndexesEntry.getValue().getIndexes()) {
                // Same reason as in above loop
                if(i.isPrimaryKey()) continue;
                
                Index new_idx = Index.create(cur_ais, cur_utable, i.getIndexName().getName(), i.getIndexId(), i.isUnique(), i.getConstraint());
                
                for(IndexColumn c : i.getColumns()) {
                    Column ref_col = cur_utable.getColumn(c.getColumn().getPosition());
                    IndexColumn icol = new IndexColumn(new_idx, ref_col, c.getPosition(), c.isAscending(), c.getIndexedLength()); 
                    new_idx.addColumn(icol);
                }
            }
            
            // Modify stored DDL statement
            DDLGenerator gen = new DDLGenerator();
            schemaManager().changeTableDefinition(session, cur_utable.getTableId(), gen.createTable(cur_utable));
            
            // And trigger build of new indexes in this table
            store().buildIndexes(session, String.format("table=(%s)", cur_utable.getName().getTableName()));
        } 
        catch(Exception e) {
            throw new InvalidOperationException(ErrorCode.UNEXPECTED_EXCEPTION, "Unexpected exception", e);
        }
    }

    @Override
    public void dropIndexes(final Session session, TableId tableId, List<Integer> indexIds) throws InvalidOperationException {
        // TODO Auto-generated method stub
    }
}
