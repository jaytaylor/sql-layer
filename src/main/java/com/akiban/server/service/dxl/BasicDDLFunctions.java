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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
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
            TableName tableName = schemaManager().createTableDefinition(session, schema, ddlText);
            checkCursorsForDDLModification(session, getAIS(session).getTable(tableName));
        } catch (Exception e) {
            InvalidOperationException ioe = launder(e);
            throwIfInstanceOf(ioe,
                    ParseException.class,
                    ProtectedTableDDLException.class,
                    UnsupportedCharsetException.class,
                    UnsupportedDataTypeException.class,
                    JoinToUnknownTableException.class,
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
            DMLFunctions dml = new BasicDMLFunctions(middleman(), this);
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
            UnsupportedDropException, GenericInvalidOperationException
    {
        logger.trace("dropping schema {}", schemaName);

        // Find all groups and tables in the schema
        Set<Group> groupsToDrop = new HashSet<Group>();
        List<UserTable> tablesToDrop = new ArrayList<UserTable>();

        final AkibanInformationSchema ais = getAIS(session);
        for(UserTable table : ais.getUserTables().values()) {
            final TableName tableName = table.getName();
            if(tableName.getSchemaName().equals(schemaName)) {
                groupsToDrop.add(table.getGroup());
                // Cannot drop entire group of parent is not in the same schema
                final Join parentJoin = table.getParentJoin();
                if(parentJoin != null) {
                    final UserTable parentTable = parentJoin.getParent();
                    if(!parentTable.getName().getSchemaName().equals(schemaName)) {
                        tablesToDrop.add(table);
                    }
                }
                // All children must be in the same schema
                for(Join childJoin : table.getChildJoins()) {
                    final TableName childName = childJoin.getChild().getName();
                    if(!childName.getSchemaName().equals(schemaName)) {
                        throw new ForeignConstraintDDLException(String.format(
                                "Cannot drop schema [%s], table [%s] has child table [%s]",
                                schemaName, tableName, childName));
                    }
                }
            }
        }
        // Remove groups that contain tables in multiple schemas
        for(UserTable table : tablesToDrop) {
            groupsToDrop.remove(table.getGroup());
        }
        // Sort table IDs so higher (i.e. children) are first
        Collections.sort(tablesToDrop, new Comparator<UserTable>() {
            @Override
            public int compare(UserTable o1, UserTable o2) {

                return o2.getTableId().compareTo(o1.getTableId());
            }
        });
        // Do the actual dropping
        for(UserTable table : tablesToDrop) {
            dropTable(session, table.getName());
        }
        for(Group group : groupsToDrop) {
            dropGroup(session, group.getName());
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
            throws NoSuchTableException, DuplicateKeyException, IndexAlterException, GenericInvalidOperationException {
        logger.trace("creating indexes {}", indexesToAdd);
        if (indexesToAdd.isEmpty() == true) {
            return;
        }

        final Table table = getTable(session, indexesToAdd.iterator().next().getTable().getName());
        try {
            schemaManager().alterTableAddIndexes(session, table.getName(), indexesToAdd);
            checkCursorsForDDLModification(session, table);
        }
        catch(InvalidOperationException e) {
            throw new IndexAlterException(e);
        }
        catch(Exception e) {
            throw new GenericInvalidOperationException(e);
        }
        
        // Build special string used/required by Store
        StringBuilder sb = new StringBuilder();
        sb.append("table=(");
        sb.append(table.getName().getTableName());
        sb.append(") ");
        
        for(Index idx : indexesToAdd) {
            sb.append("index=(");
            sb.append(idx.getIndexName().getName());
            sb.append(")");
        }

        final String indexString = sb.toString();
        try {
            store().buildIndexes(session, indexString, false);
        } catch(Exception e) {
            // Try and roll back
            List<String> indexNames = new ArrayList<String>(indexesToAdd.size());
            for(Index idx : indexesToAdd) {
                indexNames.add(idx.getIndexName().getName());
            }
            try {
                dropIndexes(session, table.getName(), indexNames);
            } catch(Exception e2) {
                logger.error("Exception while rolling back failed createIndex : " + indexString, e2);
            }
            InvalidOperationException ioe = launder(e);
            throwIfInstanceOf(ioe, DuplicateKeyException.class);
            throw new GenericInvalidOperationException(ioe);
        }
    }

    @Override
    public void dropIndexes(final Session session, TableName tableName, Collection<String> indexNamesToDrop)
            throws NoSuchTableException, IndexAlterException, GenericInvalidOperationException
    {
        logger.trace("dropping indexes {}", indexNamesToDrop);
        if(indexNamesToDrop.isEmpty() == true) {
            return;
        }

        final Table table = getAIS(session).getTable(tableName);
        if (table == null) {
            throw new NoSuchTableException(tableName);
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("table=(");
        sb.append(tableName.getTableName());
        sb.append(") ");

        // Validate and build string for Store
        for(String indexName : indexNamesToDrop) {
            Index index = table.getIndex(indexName);
            if(index == null) {
                throw new IndexAlterException(ErrorCode.NO_INDEX, "Unknown index: " + indexName);
            }
            if(index.isPrimaryKey()) {
                throw new IndexAlterException(ErrorCode.UNSUPPORTED_OPERATION, "Cannot drop primary key index");
            }
            sb.append("index=(");
            sb.append(indexName);
            sb.append(") ");
        }
        
        // Drop them from the Store before schema change while IndexDefs still exist
        store().deleteIndexes(session, sb.toString());
        
        try {
            schemaManager().alterTableDropIndexes(session, tableName, indexNamesToDrop);
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

    BasicDDLFunctions(BasicDXLMiddleman middleman) {
        super(middleman);
    }
}
