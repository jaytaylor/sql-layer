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
            schemaManager().createTableDefinition(session, schema, ddlText, false);
        } catch (Exception e) {
            InvalidOperationException ioe = launder(e);
            throwIfInstanceOf(ParseException.class, ioe);
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
    public void createIndexes(final Session session, AkibaInformationSchema ais)
            throws InvalidOperationException {
        // Can only add index to single table at a time
        if (ais.getUserTables().size() != 1) {
            throw new InvalidOperationException(ErrorCode.UNSUPPORTED_OPERATION,
                    "Can only add indexes to one table at a time");
        }

        final AkibaInformationSchema curAIS = getAIS(session);
        final Entry<TableName, UserTable> newEntry = ais.getUserTables().entrySet().iterator().next();
        final UserTable table = curAIS.getUserTable(newEntry.getKey());

        // Require existing table to modify
        if (table == null) {
            throw new InvalidOperationException(ErrorCode.UNSUPPORTED_OPERATION, "Unkown table: "
                    + newEntry.getKey().getTableName());
        }

        // Require ids match for current and proposed (some other DDL may have happend)
        if (table.getTableId().equals(newEntry.getValue().getTableId()) == false) {
            throw new InvalidOperationException(ErrorCode.UNSUPPORTED_OPERATION,
                    "TableId does not match current");
        }

        for (Index i : newEntry.getValue().getIndexes()) {
            // AIS Reader.close() adds a pkey to all UserTables that get instantiated.
            // This interface does not do pkey additions so skip it.
            if (i.isPrimaryKey()) continue;

            final String indexName = i.getIndexName().getName();
            if (table.getIndex(indexName) != null) {
                throw new InvalidOperationException(ErrorCode.UNSUPPORTED_OPERATION,
                        "Index already exists: " + indexName);
            }
        }

        StringBuilder namesSB = new StringBuilder();
        
        // All were valid, add to current AIS
        for (Index i : newEntry.getValue().getIndexes()) {
            // Same reason as in above loop
            if (i.isPrimaryKey()) continue;

            Index newIndex = Index.create(curAIS, table, i.getIndexName().getName(), -1,
                    i.isUnique(), i.getConstraint());

            for (IndexColumn c : i.getColumns()) {
                Column refCol = table.getColumn(c.getColumn().getPosition());
                IndexColumn indexCol = new IndexColumn(newIndex, refCol, c.getPosition(),
                        c.isAscending(), c.getIndexedLength());
                newIndex.addColumn(indexCol);
            }
            
            namesSB.append("index=(");
            namesSB.append(i.getIndexName());
            namesSB.append(")");
        }

        // Modify stored DDL statement
        try {
            final DDLGenerator gen = new DDLGenerator();
            final TableName tableName = table.getName();
            final String newDDL = gen.createTable(table);
            schemaManager().createTableDefinition(session, tableName.getSchemaName(), newDDL, true);

            // Trigger recreation
            schemaManager().getAis(session);

            // And trigger build of new indexes in this table
            store().buildIndexes(session,
                    String.format("table=(%s) %s", tableName.getTableName(), namesSB.toString()));
        } catch (Exception e) {
            throw new InvalidOperationException(ErrorCode.UNEXPECTED_EXCEPTION,
                    "Unexpected exception", e);
        }
    }

    @Override
    public void dropIndexes(final Session session, TableId tableId, List<Integer> indexIds) throws InvalidOperationException {
        // TODO Auto-generated method stub
    }
}
