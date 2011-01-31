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

package com.akiban.cserver.api;

import java.util.List;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.ddl.DuplicateColumnNameException;
import com.akiban.cserver.api.ddl.DuplicateTableNameException;
import com.akiban.cserver.api.ddl.ForeignConstraintDDLException;
import com.akiban.cserver.api.ddl.GroupWithProtectedTableException;
import com.akiban.cserver.api.ddl.IndexAlterException;
import com.akiban.cserver.api.ddl.JoinToUnknownTableException;
import com.akiban.cserver.api.ddl.JoinToWrongColumnsException;
import com.akiban.cserver.api.ddl.NoPrimaryKeyException;
import com.akiban.cserver.api.ddl.ParseException;
import com.akiban.cserver.api.ddl.ProtectedTableDDLException;
import com.akiban.cserver.api.ddl.UnsupportedCharsetException;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.store.SchemaId;
import com.akiban.message.ErrorCode;

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
    public void createIndexes(final Session session, List<Index> indexesToAdd)
            throws InvalidOperationException {
        if (indexesToAdd.isEmpty() == true) {
            return;
        }
        
        final Index firstIndex = indexesToAdd.get(0);
        final AkibaInformationSchema ais = getAIS(session);
        final UserTable table = ais.getUserTable(firstIndex.getTableName());
        
        if (table == null) {
            throw new IndexAlterException(ErrorCode.NO_SUCH_TABLE, "Unkown table: "
                    + firstIndex.getTableName());
        }

        // Require that IDs match for current and proposed (some other DDL may have happened)
        if (table.getTableId().equals(firstIndex.getTable().getTableId()) == false) {
            throw new IndexAlterException(ErrorCode.NO_SUCH_TABLE, "TableId mismatch");
        }
        
        // Input validation: same table, not a primary key, not a duplicate index name, and 
        // referenced columns are valid
        for (Index idx : indexesToAdd) {
            if (idx.getTable().equals(firstIndex.getTable()) == false) {
                throw new IndexAlterException(ErrorCode.UNSUPPORTED_OPERATION,
                        "Cannot add indexes to multiple tables");
            }

            if (idx.isPrimaryKey()) {
                throw new IndexAlterException(ErrorCode.UNSUPPORTED_OPERATION,
                        "Cannot add primary key");
            }

            final String indexName = idx.getIndexName().getName();
            if (table.getIndex(indexName) != null) {
                throw new IndexAlterException(ErrorCode.DUPLICATE_KEY,
                        "Index name already exists: " + indexName);
            }
            
            for (IndexColumn idxCol : idx.getColumns()) {
                final Column refCol = idxCol.getColumn();
                final Column tableCol = table.getColumn(refCol.getPosition());
                if (refCol.getName().equals(tableCol.getName()) == false || 
                    refCol.getType().equals(tableCol.getType()) == false) {
                    throw new IndexAlterException(ErrorCode.UNSUPPORTED_OPERATION,
                            "Index column does not match table column");
                }
            }
        }
         
        StringBuilder nameBuffer = new StringBuilder();
        for (Index idx : indexesToAdd) {
            // Add to current table/AIS so that the DDLGenerator call below will see it
            Index newIndex = Index.create(ais, table, idx.getIndexName().getName(), -1,
                    idx.isUnique(), idx.getConstraint());

            for (IndexColumn idxCol : idx.getColumns()) {
                Column refCol = table.getColumn(idxCol.getColumn().getPosition());
                IndexColumn indexCol = new IndexColumn(newIndex, refCol, idxCol.getPosition(),
                        idxCol.isAscending(), idxCol.getIndexedLength());
                newIndex.addColumn(indexCol);
            }
            
            // Track new index names to build only new indexes
            nameBuffer.append("index=(");
            nameBuffer.append(idx.getIndexName());
            nameBuffer.append(")");
        }

        
        try {
            // Generate new DDL statement from existing AIS/table
            final DDLGenerator gen = new DDLGenerator();
            final TableName tableName = table.getName();
            final String newDDL = gen.createTable(table);
            
            // Store new DDL statement and recreate AIS
            schemaManager().createTableDefinition(session, tableName.getSchemaName(), newDDL, true);
            schemaManager().getAis(session);

            // Trigger build of new indexes in this table
            store().buildIndexes(session,
                    String.format("table=(%s) %s", tableName.getTableName(), nameBuffer.toString()));
        } catch (Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }

    @Override
    public void dropIndexes(final Session session, TableId tableId, List<Integer> indexesToDrop)
            throws InvalidOperationException {
        throw new UnsupportedOperationException("Unimplemented");
    }
}
