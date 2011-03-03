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

package com.akiban.server.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowDef;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.api.ddl.DuplicateColumnNameException;
import com.akiban.server.api.ddl.DuplicateTableNameException;
import com.akiban.server.api.ddl.ForeignConstraintDDLException;
import com.akiban.server.api.ddl.GroupWithProtectedTableException;
import com.akiban.server.api.ddl.IndexAlterException;
import com.akiban.server.api.ddl.JoinToUnknownTableException;
import com.akiban.server.api.ddl.JoinToWrongColumnsException;
import com.akiban.server.api.ddl.NoPrimaryKeyException;
import com.akiban.server.api.ddl.ParseException;
import com.akiban.server.api.ddl.ProtectedTableDDLException;
import com.akiban.server.api.ddl.UnsupportedCharsetException;
import com.akiban.server.api.ddl.UnsupportedDataTypeException;
import com.akiban.server.api.ddl.UnsupportedDropException;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.SchemaId;
import com.akiban.server.util.RowDefNotFoundException;
import com.akiban.message.ErrorCode;

public class DDLFunctionsImpl extends ClientAPIBase implements
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
            DuplicateColumnNameException, UnsupportedDataTypeException,
            GenericInvalidOperationException {
        try {
            schemaManager().createTableDefinition(session, schema, ddlText, false);
        } catch (Exception e) {
            InvalidOperationException ioe = launder(e);
            throwIfInstanceOf(ParseException.class, ioe);
            throwIfInstanceOf(UnsupportedDataTypeException.class, ioe);
            throw new GenericInvalidOperationException(ioe);
        }
    }

    @Override
    public void dropTable(Session session, TableName tableName)
            throws ProtectedTableDDLException, ForeignConstraintDDLException,
            UnsupportedDropException, GenericInvalidOperationException {
        final Table table = getAIS(session).getTable(tableName);
        
        if(table == null) {
            return; // dropping a non-existing table is a no-op
        }

        final UserTable userTable = table.isUserTable() ? (UserTable)table : null;

        // Halo spec: may only drop leaf tables through DDL interface
        if(userTable == null || userTable.getChildJoins().isEmpty() == false) {
            throw new UnsupportedDropException(String.format("Cannot drop non-leaf table [%s]", table.getName()));
        }

        try {
            DMLFunctions dml = dmlFunctionsForDrop();
            dml.truncateTable(session, table.getTableId());
            schemaManager().deleteTableDefinition(session, tableName.getSchemaName(), tableName.getTableName());
        } catch (Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }

    protected DMLFunctions dmlFunctionsForDrop() {
        return new DMLFunctionsImpl(this);
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
    public void dropGroup(Session session, String groupName)
            throws ProtectedTableDDLException, GenericInvalidOperationException, NoSuchTableException,
            UnsupportedDropException, ForeignConstraintDDLException {
        List<Integer> toDrop = new ArrayList<Integer>();
        for(UserTable userTable : getAIS(session).getUserTables().values()) {
            if(userTable.getGroup().getName().equals(groupName)) {
                toDrop.add(userTable.getTableId());
            }
        }

        // Child tables always have a higher id than parents, reverse sort to drop children first
        Collections.sort(toDrop, Collections.reverseOrder());
        for(Integer id : toDrop) {
            dropTable(session, getTableName(session, id));
        }
    }

    @Override
    public AkibanInformationSchema getAIS(final Session session) {
        return schemaManager().getAis(session);
    }

    @Override
    public int getTableId(Session session, TableName tableName) throws NoSuchTableException {
        Table table = getAIS(session).getTable(tableName);
        if (table == null) {
            throw new NoSuchTableException(tableName);
        }
        return table.getTableId();
    }

    @Override
    public Table getTable(Session session, int tableId) throws NoSuchTableException {
        for (Table userTable : getAIS(session).getUserTables().values()) {
            if (tableId == userTable.getTableId()) {
                return userTable;
            }
        }
        for (Table groupTable : getAIS(session).getGroupTables().values()) {
            if (tableId == groupTable.getTableId()) {
                return groupTable;
            }
        }
        throw new NoSuchTableException(tableId);
    }

    @Override
    public Table getTable(Session session, TableName tableName) throws NoSuchTableException {
        AkibanInformationSchema ais = getAIS(session);
        Table table = ais.getTable(tableName);
        if (table == null) {
            throw new NoSuchTableException(tableName);
        }
        return table;
    }

    @Override
    public UserTable getUserTable(Session session, TableName tableName) throws NoSuchTableException {
        AkibanInformationSchema ais = getAIS(session);
        UserTable table = ais.getUserTable(tableName);
        if (table == null) {
            throw new NoSuchTableException(tableName);
        }
        return table;
    }

    @Override
    public TableName getTableName(Session session, int tableId) throws NoSuchTableException {
        return getTable(session, tableId).getName();
    }

    @Override
    public RowDef getRowDef(int tableId) throws NoSuchTableException {
        try {
            return store().getRowDefCache().getRowDef(tableId);
        } catch (RowDefNotFoundException e) {
            throw new NoSuchTableException(tableId, e);
        }
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
    public void createIndexes(final Session session, Collection<Index> indexesToAdd)
            throws InvalidOperationException {
        if (indexesToAdd.isEmpty() == true) {
            return;
        }
        
        final Index firstIndex = indexesToAdd.iterator().next();
        final AkibanInformationSchema ais = getAIS(session);
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
         
        StringBuilder sb = new StringBuilder();
        sb.append("table=(");
        sb.append(table.getName().getTableName());
        sb.append(") ");
        
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
            sb.append("index=(");
            sb.append(idx.getIndexName());
            sb.append(")");
        }

        try {
            // Generate new DDL statement from existing AIS/table
            final DDLGenerator gen = new DDLGenerator();
            final TableName tableName = table.getName();
            final String newDDL = gen.createTable(table);
            
            // Store new DDL statement and recreate AIS
            schemaManager().createTableDefinition(session, tableName.getSchemaName(), newDDL, true);

            // Trigger build of new index trees
            store().buildIndexes(session, sb.toString());
        } catch (Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }

    @Override
    public void dropIndexes(final Session session, TableName tableName, Collection<String> indexNamesToDrop)
            throws InvalidOperationException {
        if(indexNamesToDrop.isEmpty() == true) {
            return;
        }

        final Table table = getAIS(session).getTable(tableName);
        if (table == null) {
            throw new IndexAlterException(ErrorCode.NO_SUCH_TABLE, "Unkown table");
        }

        ArrayList<Index> indexesToDrop = new ArrayList<Index>();

        StringBuilder sb = new StringBuilder();
        sb.append("table=(");
        sb.append(tableName.getTableName());
        sb.append(") ");

        // Confirm they exist
        for(String indexName : indexNamesToDrop) {
            Index index = table.getIndex(indexName);
            if(index == null) {
                throw new IndexAlterException(ErrorCode.NO_INDEX, "Unkown index: " + indexName);
            }
            if(index.isPrimaryKey() == true) {
                throw new IndexAlterException(ErrorCode.UNSUPPORTED_OPERATION,
                        "Cannot drop primary key index");
            }
            indexesToDrop.add(index);
            sb.append("index=(");
            sb.append(indexName);
            sb.append(") ");
        }

        // Remove from existing AIS to generate new DDL
        if(table.isUserTable()) {
            ((UserTable)table).getIndexesIncludingInternal().removeAll(indexesToDrop);
        }
        else {
            table.getIndexes().removeAll(indexesToDrop);
        }
        
        try {
            // Generate new DDL statement from existing AIS/table
            final DDLGenerator gen = new DDLGenerator();
            final String newDDL = gen.createTable(table);

            // Trigger drop of index trees while indexDef(s) still exist
            store().deleteIndexes(session, sb.toString());
            
            // Store new DDL statement and recreate AIS
            schemaManager().createTableDefinition(session, tableName.getSchemaName(), newDDL, true);
        } catch(Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }
}
