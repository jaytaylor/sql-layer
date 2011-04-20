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

package com.akiban.server.service.dxl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowDef;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.GenericInvalidOperationException;
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
import com.akiban.server.api.dml.scan.Cursor;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.service.session.Session;
import com.akiban.server.util.RowDefNotFoundException;
import com.akiban.message.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.akiban.server.service.dxl.BasicDXLMiddleman.getScanDataMap;
import static com.akiban.util.Exceptions.throwIfInstanceOf;

class BasicDDLFunctions extends ClientAPIBase implements DDLFunctions {

    private final static Logger logger = LoggerFactory.getLogger(BasicDDLFunctions.class);

    @Override
    public void createTable(Session session, String schema, String ddlText)
            throws ParseException, UnsupportedCharsetException,
            ProtectedTableDDLException, DuplicateTableNameException,
            GroupWithProtectedTableException, JoinToUnknownTableException,
            JoinToWrongColumnsException, JoinToMultipleParentsException,
            NoPrimaryKeyException, DuplicateColumnNameException,
            UnsupportedDataTypeException, UnsupportedIndexDataTypeException,
            UnsupportedIndexSizeException, GenericInvalidOperationException
    {
        logger.trace("creating table: ({}) {}", schema, ddlText);
        try {
            TableName tableName = schemaManager().createTableDefinition(session, schema, ddlText, false);
            checkCursorsForDDLModification(session, getAIS(session).getTable(tableName));
        } catch (Exception e) {
            InvalidOperationException ioe = launder(e);
            throwIfInstanceOf(ioe,
                    ParseException.class,
                    UnsupportedDataTypeException.class,
                    JoinToWrongColumnsException.class,
                    JoinToMultipleParentsException.class,
                    DuplicateTableNameException.class,
                    UnsupportedIndexDataTypeException.class,
                    UnsupportedIndexSizeException.class
            );
            throw new GenericInvalidOperationException(ioe);
        }
    }

    @Override
    public void dropTable(Session session, TableName tableName)
            throws ProtectedTableDDLException, ForeignConstraintDDLException,
            UnsupportedDropException, GenericInvalidOperationException
    {
        logger.trace("dropping table {}", tableName);
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
            DMLFunctions dml = new BasicDMLFunctions(this);
            dml.truncateTable(session, table.getTableId());
            schemaManager().deleteTableDefinition(session, tableName.getSchemaName(), tableName.getTableName());
            checkCursorsForDDLModification(session, table);
        } catch (Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }

    @Override
    public void dropSchema(Session session, String schemaName)
            throws ProtectedTableDDLException, ForeignConstraintDDLException,
            GenericInvalidOperationException
    {
        logger.trace("dropping schema {}", schemaName);
        try {
            schemaManager().deleteSchemaDefinition(session, schemaName);
        } catch (Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }

    @Override
    public void dropGroup(Session session, String groupName)
            throws ProtectedTableDDLException, GenericInvalidOperationException
    {
        logger.trace("dropping group {}", groupName);
        final Group group = getAIS(session).getGroup(groupName);
        if(group == null) {
            return;
        }
        try {
            final Table table = group.getGroupTable();
            final RowDef rowDef = getRowDef(table.getTableId());
            final TableName tableName = table.getName();
            store().truncateGroup(session, rowDef.getRowDefId());
            schemaManager().deleteTableDefinition(session, tableName.getSchemaName(), tableName.getTableName());
            checkCursorsForDDLModification(session, table);
        } catch(Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }

    @Override
    public AkibanInformationSchema getAIS(final Session session) {
        logger.trace("getting AIS");
        return schemaManager().getAis(session);
    }

    @Override
    public int getTableId(Session session, TableName tableName) throws NoSuchTableException{
        logger.trace("getting table ID for {}", tableName);
        Table table = getAIS(session).getTable(tableName);
        if (table == null) {
            throw new NoSuchTableException(tableName);
        }
        return table.getTableId();
    }

    @Override
    public Table getTable(Session session, int tableId) throws NoSuchTableException {
        logger.trace("getting AIS Table for {}", tableId);
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
        logger.trace("getting AIS Table for {}", tableName);
        AkibanInformationSchema ais = getAIS(session);
        Table table = ais.getTable(tableName);
        if (table == null) {
            throw new NoSuchTableException(tableName);
        }
        return table;
    }

    @Override
    public UserTable getUserTable(Session session, TableName tableName) throws NoSuchTableException {
        logger.trace("getting AIS UserTable for {}", tableName);
        AkibanInformationSchema ais = getAIS(session);
        UserTable table = ais.getUserTable(tableName);
        if (table == null) {
            throw new NoSuchTableException(tableName);
        }
        return table;
    }

    @Override
    public TableName getTableName(Session session, int tableId) throws NoSuchTableException {
        logger.trace("getting table name for {}", tableId);
        return getTable(session, tableId).getName();
    }

    @Override
    public RowDef getRowDef(int tableId) throws NoSuchTableException {
        logger.trace("getting RowDef for {}", tableId);
        try {
            return store().getRowDefCache().getRowDef(tableId);
        } catch (RowDefNotFoundException e) {
            throw new NoSuchTableException(tableId, e);
        }
    }

    @Override
    public List<String> getDDLs(final Session session) throws InvalidOperationException {
        logger.trace("getting DDLs");
        try {
            return schemaManager().schemaStrings(session, false);
        } catch (Exception e) {
            throw new InvalidOperationException(ErrorCode.UNEXPECTED_EXCEPTION,
                    "Unexpected exception", e);
        }
    }

    @Override
    public int getGeneration() {
        return schemaManager().getSchemaGeneration();
    }

    @Override
    @SuppressWarnings("unused")
    // meant to be used from JMX
    public void forceGenerationUpdate() {
        logger.trace("forcing schema generation update");
        schemaManager().forceNewTimestamp();
    }

    @Override
    public void createIndexes(final Session session, Collection<Index> indexesToAdd)
            throws InvalidOperationException
    {
        logger.trace("creating indexes {}", indexesToAdd);
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
        
        // Save in case of error
        final DDLGenerator gen = new DDLGenerator();
        final String originalDDL = gen.createTable(table);

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
            newIndex.freezeColumns();
            
            // Track new index names to build only new indexes
            sb.append("index=(");
            sb.append(idx.getIndexName());
            sb.append(")");
        }

        final String schemaName = table.getName().getSchemaName();

        try {
            // Generate new DDL statement from existing AIS/table
            final String newDDL = gen.createTable(table);
            schemaManager().createTableDefinition(session, schemaName, newDDL, true);
            checkCursorsForDDLModification(session, table);
        } catch (Exception e) {
            throw new GenericInvalidOperationException(e);
        }

        final String indexString = sb.toString();
        try {
            // Trigger build of new index trees
            store().buildIndexes(session, indexString);
        } catch(Exception e) {
            try {
                // Delete whatever was inserted, roll back table change
                store().deleteIndexes(session, indexString);
                schemaManager().createTableDefinition(session, schemaName, originalDDL, true);
            } catch(Exception e2) {
                logger.error("Exception while rolling back failed createIndex {}: {}",
                             indexString, e2);
            }
            InvalidOperationException ioe = launder(e);
            throwIfInstanceOf(ioe, DuplicateKeyException.class);
            throw new GenericInvalidOperationException(ioe);
        }
    }

    @Override
    public void dropIndexes(final Session session, TableName tableName, Collection<String> indexNamesToDrop)
            throws InvalidOperationException
    {
        logger.trace("dropping indexes {}", indexNamesToDrop);
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
        table.removeIndexes(indexesToDrop);
        
        try {
            // Generate new DDL statement from existing AIS/table
            final DDLGenerator gen = new DDLGenerator();
            final String newDDL = gen.createTable(table);

            // Trigger drop of index trees while indexDef(s) still exist
            store().deleteIndexes(session, sb.toString());
            
            // Store new DDL statement and recreate AIS
            schemaManager().createTableDefinition(session, tableName.getSchemaName(), newDDL, true);
            checkCursorsForDDLModification(session, table);
        } catch(Exception e) {
            throw new GenericInvalidOperationException(e);
        }
    }

    private void checkCursorsForDDLModification(Session session, Table table) throws NoSuchTableException {
        Map<CursorId,BasicDXLMiddleman.ScanData> cursorsMap = getScanDataMap(session);
        if (cursorsMap == null) {
            return;
        }

        final int tableId;
        final int gTableId;
        {
            if (table.isUserTable()) {
                tableId = table.getTableId();
                gTableId = table.getGroup().getGroupTable().getTableId();
            }
            else {
                tableId = gTableId = table.getTableId();
            }
        }

        for (BasicDXLMiddleman.ScanData scanData : cursorsMap.values()) {
            Cursor cursor = scanData.getCursor();
            if (cursor.isClosed()) {
                continue;
            }
            ScanRequest request = cursor.getScanRequest();
            int scanTableId = request.getTableId();
            if (scanTableId == tableId || scanTableId == gTableId) {
                cursor.setDDLModified();
            }
        }
    }
}
