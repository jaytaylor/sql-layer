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

package com.akiban.server.store;

import static com.akiban.server.service.tree.TreeService.SCHEMA_TREE_NAME;
import static com.akiban.server.service.tree.TreeService.STATUS_TREE_NAME;
import static com.akiban.server.store.PersistitStore.MAX_TRANSACTION_RETRY_COUNT;

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.akiban.ais.io.AISTarget;
import com.akiban.ais.io.MessageSource;
import com.akiban.ais.io.MessageTarget;
import com.akiban.ais.io.Reader;
import com.akiban.ais.io.TableSubsetWriter;
import com.akiban.ais.io.Writer;
import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AISMerge;
import com.akiban.ais.model.AISTableNameChanger;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.HKey;
import com.akiban.ais.model.HKeyColumn;
import com.akiban.ais.model.HKeySegment;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.Type;
import com.akiban.server.encoding.EncoderFactory;
import com.akiban.server.error.AISTooLargeException;
import com.akiban.server.error.BranchingGroupIndexException;
import com.akiban.server.error.DuplicateIndexException;
import com.akiban.server.error.DuplicateTableNameException;
import com.akiban.server.error.IndexLacksColumnsException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.JoinColumnMismatchException;
import com.akiban.server.error.JoinColumnTypesMismatchException;
import com.akiban.server.error.JoinToMultipleParentsException;
import com.akiban.server.error.JoinToProtectedTableException;
import com.akiban.server.error.JoinToUnknownTableException;
import com.akiban.server.error.JoinToWrongColumnsException;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchGroupException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.ParseException;
import com.akiban.server.error.PersistItErrorException;
import com.akiban.server.error.ProtectedIndexException;
import com.akiban.server.error.ProtectedTableDDLException;
import com.akiban.server.error.ReferencedTableException;
import com.akiban.server.error.ScanRetryAbandonedException;
import com.akiban.server.error.SchemaLoadIOException;
import com.akiban.server.error.TableNotInGroupException;
import com.akiban.server.error.UnsupportedCharsetException;
import com.akiban.server.error.UnsupportedDataTypeException;
import com.akiban.server.error.UnsupportedIndexDataTypeException;
import com.akiban.server.error.UnsupportedIndexPrefixException;
import com.akiban.server.error.UnsupportedIndexSizeException;
import com.akiban.server.rowdata.RowDefCache;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeLink;
import com.google.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.ddl.SchemaDef;
import com.akiban.ais.ddl.SchemaDefToAis;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.server.AkServer;
import com.akiban.server.service.Service;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.service.tree.TreeVisitor;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Transaction;
import com.persistit.Transaction.DefaultCommitListener;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TransactionFailedException;

public class PersistitStoreSchemaManager implements Service<SchemaManager>,
        SchemaManager {

    final static int AIS_BASE_TABLE_ID = 1000000000;
    
    static final String AIS_DDL_NAME = "akiban_information_schema.ddl";

    static final String BY_AIS = "byAIS";

    private static final Logger LOG = LoggerFactory
            .getLogger(PersistitStoreSchemaManager.class.getName());

    private final static String CREATE_SCHEMA_FORMATTER = "create schema if not exists `%s`;";

    private final static boolean forceToDisk = true;

    /**
     * Maximum size that can can be stored in an index. See
     * {@link Transaction#prepareTxnExchange(com.persistit.Tree, com.persistit.Key, char)}
     * for details on upper bound.
     */
    static final int MAX_INDEX_STORAGE_SIZE = Key.MAX_KEY_LENGTH - 32;

    /**
     * Maximum size for an ordinal value as stored with the HKey. Note that this
     * <b>must match</b> the EWIDTH_XXX definition from {@link Key}, where XXX
     * is the return type of {@link RowDef#getOrdinal()}. Currently this is
     * int and {@link Key#EWIDTH_INT}.
     */
    static final int MAX_ORDINAL_STORAGE_SIZE = 5;

    private static final String COMMIT_AIS_ERROR_MSG = "INTERNAL INCONSISTENCY, error while building RowDefCache";

    public static final String MAX_AIS_SIZE_PROPERTY = "akserver.max_ais_size_bytes";

    private static final String TREE_NAME_SEPARATOR = "$$";

    private static final AtomicInteger indexCounter = new AtomicInteger(0);

    private interface AISChangeCallback {
        public void beforeCommit(Exchange schemaExchange, TreeService treeService);
    }

    private final AisHolder aish;
    private final SessionService sessionService;
    private final Store store;
    private final TreeService treeService;
    private final ConfigurationService config;
    private AtomicLong updateTimestamp;
    private int maxAISBufferSize;
    private ByteBuffer aisByteBuffer;

    @Inject
    public PersistitStoreSchemaManager(AisHolder aisHolder, ConfigurationService config, SessionService sessionService, Store store, TreeService treeService) {
        this.aish = aisHolder;
        this.config = config;
        this.sessionService = sessionService;
        this.treeService = treeService;
        this.store = store;
    }

    private static String computeTreeName(Group group, Index index) {
        IndexName iName = index.getIndexName();
        return group.getName() + TREE_NAME_SEPARATOR +
               iName.getSchemaName() + TREE_NAME_SEPARATOR +
               iName.getTableName() + TREE_NAME_SEPARATOR +
               iName.getName() + TREE_NAME_SEPARATOR +
               indexCounter.getAndIncrement(); // Ensure uniqueness when tables are copied, (e.g. rename, alter table).
    }

    /**
     * The AISBuilder normally sets tree names for GroupTable indexes but on a brand new table,
     * those indexes don't have tree names yet (they are set afterwards). This syncs up all the
     * tree names on the given GroupTable.
     * @param groupTable GroupTable to fix up
     */
    private static void syncIndexTreeNames(GroupTable groupTable) {
        for(TableIndex index : groupTable.getIndexes()) {
            if(!index.getColumns().isEmpty()) {
                Column groupColumn = index.getColumns().get(0).getColumn();
                Column userColumn = groupColumn.getUserColumn();
                assert userColumn != null : groupColumn;
                boolean found = false;
                for(TableIndex userIndex : userColumn.getUserTable().getIndexesIncludingInternal()) {
                    if(userIndex.getIndexId().equals(index.getIndexId())) {
                        index.setTreeName(userIndex.getTreeName());
                        found = true;
                        break;
                    }
                }
                assert found : index;
            }
        }
    }

    // TODO: Cleanup {@link #createIndexes(Session, Collection)} so AISBuilder can set final tree name
    // on an index and {@link #syncIndexTreeNames(GroupTable)} could go away entirely
    private static void setTreeNames(UserTable newTable) {
        Group group = newTable.getGroup();
        GroupTable groupTable = group.getGroupTable();
        newTable.setTreeName(groupTable.getTreeName());
        for(TableIndex index : newTable.getIndexesIncludingInternal()) {
            index.setTreeName(computeTreeName(group, index));
        }
        syncIndexTreeNames(groupTable);
    }

    
    @Override
    public TableName createTableDefinition(Session session, final UserTable newTable)
    {
        AISMerge merge = new AISMerge (getAis(), newTable);
        merge.merge();
        
        final String schemaName = newTable.getName().getSchemaName();
        final UserTable finalTable = merge.getAIS().getUserTable(newTable.getName());
        //validateIndexSizes(newTable);
        setTreeNames(finalTable);
        
        try {
            commitAISChange(session, merge.getAIS(), schemaName, null);
        } catch (PersistitException ex) {
            throw new PersistItErrorException (ex);
        }
            
        return newTable.getName();
    }
    
    @Override
    public TableName createTableDefinition(final Session session, final String defaultSchemaName,
                                           final String originalDDL) {
        String ddlStatement = originalDDL;
        SchemaDef schemaDef = parseTableStatement(defaultSchemaName, ddlStatement);
        SchemaDef.UserTableDef tableDef = schemaDef.getCurrentTable();
        if (tableDef.isLikeTableDef()) {
            final SchemaDef.CName srcName = tableDef.getLikeCName();
            assert srcName.getSchema() != null : originalDDL;
            final Table srcTable = aish.getAis().getTable(srcName.getSchema(), srcName.getName());
            if (srcTable == null) {
                throw new NoSuchTableException (srcName.getSchema(), srcName.getName());
            }
            final SchemaDef.CName dstName = tableDef.getCName();
            assert dstName.getSchema() != null : originalDDL;
            DDLGenerator gen = new DDLGenerator(dstName.getSchema(), dstName.getName());
            ddlStatement = gen.createTable(srcTable);
            schemaDef = parseTableStatement(defaultSchemaName, ddlStatement);
            tableDef = schemaDef.getCurrentTable();
        }

        final String schemaName = tableDef.getCName().getSchema();
        final String tableName = tableDef.getCName().getName();
        final Table curTable = getAis().getTable(schemaName, tableName);
        if (curTable != null) {
            throw new DuplicateTableNameException (new TableName(schemaName, tableName));
        }

        validateTableDefinition(tableDef);
        final AkibanInformationSchema newAIS = addSchemaDefToAIS(schemaDef);
        final UserTable newTable = newAIS.getUserTable(schemaName, tableName);
        
        //TODO: remove this (and the definition) when the AISValidate is turned on
        validateIndexSizes(newTable);
        setTreeNames(newTable);
        
        // TODO: A dozen or two ITs need updated before can validate through this (or mostly NOT NULL
        // primary key parts, a few modifications of frozen AIS)
        //newAIS.validate(AISValidations.LIVE_AIS_VALIDATIONS).throwIfNecessary();
        //newAIS.freeze();
        try {
            commitAISChange(session, newAIS, schemaName, null);
        } catch (PersistitException ex) {
            throw new PersistItErrorException (ex);
        }
        return newTable.getName();
    }

    @Override
    public void renameTable(Session session, TableName currentName, TableName newName) {
        String curSchema = currentName.getSchemaName();
        String newSchema = newName.getSchemaName();
        if(curSchema.equals(TableName.AKIBAN_INFORMATION_SCHEMA)) {
            throw new ProtectedTableDDLException(currentName);
        }
        if(newSchema.equals(TableName.AKIBAN_INFORMATION_SCHEMA)) {
            throw new ProtectedTableDDLException(newName);
        }

        UserTable curTable = getAis().getUserTable(currentName);
        UserTable newTable = getAis().getUserTable(newName);
        if(curTable == null) {
            throw new NoSuchTableException(currentName.getSchemaName(), currentName.getTableName());
        }
        if(newTable != null) {
            throw new DuplicateTableNameException(newName);
        }

        final AkibanInformationSchema newAIS = new AkibanInformationSchema();
        new Writer(new AISTarget(newAIS)).save(getAis());
        newTable = newAIS.getUserTable(currentName);
        
        AISTableNameChanger nameChanger = new AISTableNameChanger(newTable);
        nameChanger.setSchemaName(newName.getSchemaName());
        nameChanger.setNewTableName(newName.getTableName());
        nameChanger.doChange();
        
        String vol1 = getVolumeForSchemaTree(currentName.getSchemaName());
        String vol2 = getVolumeForSchemaTree(newName.getSchemaName());

        try {
            commitAISChange(session, newAIS, currentName.getSchemaName(), null);
            if(!vol1.equals(vol2)) {
                commitAISChange(session, newAIS, newName.getSchemaName(), null);
            }
        } catch (PersistitException ex) {
            throw new PersistItErrorException (ex);
        }
    }

    private static boolean inSameBranch(UserTable t1, UserTable t2) {
        if(t1 == t2) {
            return true;
        }
        // search for t2 in t1->root
        Join join = t1.getParentJoin();
        while(join != null) {
            final UserTable parent = join.getParent();
            if(parent == t2) {
                return true;
            }
            join = parent.getParentJoin();
        }
        // search fo t1 in t2->root
        join = t2.getParentJoin();
        while(join != null) {
            final UserTable parent = join.getParent();
            if(parent == t1) {
                return true;
            }
            join = parent.getParentJoin();
        }
        return false;
    }
    
    @Override
    public Collection<Index> createIndexes(Session session, Collection<? extends Index> indexesToAdd) {
        final Map<String,String> volumeToSchema = new HashMap<String,String>();
        final AkibanInformationSchema newAIS = new AkibanInformationSchema();
        new Writer(new AISTarget(newAIS)).save(getAis());
        final AISBuilder builder = new AISBuilder(newAIS);

        final List<Index> newIndexes = new ArrayList<Index>();

        for(Index index : indexesToAdd) {
            final IndexName indexName = index.getIndexName();
            if(index.isPrimaryKey()) {
                throw new ProtectedIndexException("PRIMARY", new TableName(indexName.getSchemaName(), indexName.getTableName()));
            }

            final Index curIndex;
            final Index newIndex;
            final Group newGroup;
            final String schemaName;

            switch(index.getIndexType()) {
                case TABLE:
                {
                    final TableName tableName = new TableName(indexName.getSchemaName(), indexName.getTableName());
                    final UserTable newTable = newAIS.getUserTable(tableName);
                    if(newTable == null) {
                        throw new NoSuchTableException(tableName);
                    }
                    curIndex = newTable.getIndex(indexName.getName());
                    schemaName = indexName.getSchemaName();
                    newGroup = newTable.getGroup();
                    Integer newId = SchemaDefToAis.findMaxIndexIDInGroup(newGroup) + 1;
                    newIndex = TableIndex.create(newAIS, newTable, indexName.getName(), newId, index.isUnique(),
                                                 index.getConstraint());
                }
                break;
                case GROUP:
                {
                    newGroup = newAIS.getGroup(indexName.getTableName());
                    if(newGroup == null) {
                        throw new NoSuchGroupException(indexName.getTableName());
                    }
                    curIndex = newGroup.getIndex(indexName.getName());
                    schemaName = indexName.getSchemaName();
                    Integer newId = SchemaDefToAis.findMaxIndexIDInGroup(newGroup) + 1;
                    newIndex = GroupIndex.create(newAIS, newGroup, indexName.getName(), newId, index.isUnique(),
                                                 index.getConstraint(), index.getJoinType());
                }
                break;
                default:
                    throw new IllegalArgumentException("Unknown index type: " + index);
            }

            if(curIndex != null) {
                throw new DuplicateIndexException(indexName);
            }
            if(index.getColumns().isEmpty()) {
                throw new IndexLacksColumnsException (
                        new TableName(index.getIndexName().getSchemaName(), index.getIndexName().getTableName()),
                        index.getIndexName().getName());
            }

            volumeToSchema.put(getVolumeForSchemaTree(schemaName), schemaName);

            UserTable lastTable = null;
            for(IndexColumn indexCol : index.getColumns()) {
                final TableName refTableName = indexCol.getColumn().getTable().getName();
                final UserTable newRefTable = newAIS.getUserTable(refTableName);
                if(newRefTable == null) {
                    throw new NoSuchTableException(refTableName);
                }
                if(!newRefTable.getGroup().equals(newGroup)) {
                    throw new TableNotInGroupException (refTableName);
                }
                // TODO: Checked in newIndex.addColumn(newIndexCol) ?  
                if(lastTable != null && !inSameBranch(lastTable, newRefTable)) {
                    throw new BranchingGroupIndexException (
                            index.getIndexName().getName(),
                            lastTable.getName(), newRefTable.getName());
                }
                lastTable = newRefTable;

                final Column column = indexCol.getColumn();
                final Column newColumn = newRefTable.getColumn(column.getName());
                if(newColumn == null) {
                    throw new NoSuchColumnException (column.getName());
                }
                if(!column.getType().equals(newColumn.getType())) {
                    throw new JoinColumnTypesMismatchException (
                            new TableName (index.getIndexName().getSchemaName(), index.getIndexName().getTableName()),
                            column.getName(),
                            newRefTable.getName(), newColumn.getName());
                }
                IndexColumn newIndexCol = new IndexColumn(newIndex, newColumn, indexCol.getPosition(),
                                                          indexCol.isAscending(), indexCol.getIndexedLength());
                newIndex.addColumn(newIndexCol);
            }

            newIndex.freezeColumns();
            newIndex.setTreeName(computeTreeName(newGroup, newIndex));
            newIndexes.add(newIndex);
            builder.generateGroupTableIndexes(newGroup);
        }

        for(String schema : volumeToSchema.values()) {
            try {
                commitAISChange(session, newAIS, schema, null);
            } catch (PersistitException ex) {
                throw new PersistItErrorException (ex);
            }
        }
        
        return newIndexes;
    }

    @Override
    public void dropIndexes(Session session, Collection<Index> indexesToDrop) {
        final AkibanInformationSchema newAIS = new AkibanInformationSchema();
        new Writer(new AISTarget(newAIS)).save(getAis());
        final AISBuilder builder = new AISBuilder(newAIS);
        final Map<String,String> volumeToSchema = new HashMap<String,String>();

        for(Index index : indexesToDrop) {
            final IndexName name = index.getIndexName();
            String schemaName = null;
            switch(index.getIndexType()) {
                case TABLE:
                    Table newTable = newAIS.getUserTable(new TableName(name.getSchemaName(), name.getTableName()));
                    if(newTable != null) {
                        schemaName = newTable.getName().getSchemaName();
                        newTable.removeIndexes(Collections.singleton(newTable.getIndex(name.getName())));
                        builder.generateGroupTableIndexes(newTable.getGroup());
                    }
                break;
                case GROUP:
                    Group newGroup = newAIS.getGroup(name.getTableName());
                    if(newGroup != null) {
                        schemaName = newGroup.getGroupTable().getName().getSchemaName();
                        newGroup.removeIndexes(Collections.singleton(newGroup.getIndex(name.getName())));
                    }
                break;
                default:
                    throw new IllegalArgumentException("Unknown index type: " + index);
            }

            volumeToSchema.put(getVolumeForSchemaTree(schemaName), schemaName);
        }

        for(String schema : volumeToSchema.values()) {
            try {
                commitAISChange(session, newAIS, schema, null);
            } catch (PersistitException ex) {
                throw new PersistItErrorException (ex);
            }
        }
    }


    @Override
    public void deleteTableDefinition(final Session session, final String schemaName,
                                      final String tableName) {
        if (TableName.AKIBAN_INFORMATION_SCHEMA.equals(schemaName)) {
            return;
        }

        final Table table = getAis().getTable(schemaName, tableName);
        if (table == null) {
            return;
        }

        final List<TableName> tables = new ArrayList<TableName>();
        if (table.isGroupTable() == true) {
            final Group group = table.getGroup();
            tables.add(group.getGroupTable().getName());
            for (final Table t : getAis().getUserTables().values()) {
                if (t.getGroup().equals(group)) {
                    tables.add(t.getName());
                }
            }
        } else if (table.isUserTable() == true) {
            final UserTable userTable = (UserTable) table;
            if (userTable.getChildJoins().isEmpty() == false) {
                throw new ReferencedTableException (table);
            }
            if (userTable.getParentJoin() == null) {
                // Last table in group, also delete group table
                tables.add(userTable.getGroup().getGroupTable().getName());
            }
            tables.add(table.getName());
        }

        final AkibanInformationSchema newAIS = removeTablesFromAIS(tables);
        try {
            commitAISChange(session, newAIS,  schemaName, new AISChangeCallback() {
                @Override
                public void beforeCommit(Exchange schemaExchange, TreeService treeService) {
                    final TreeLink statusTreeLink = treeService.treeLink(schemaName, STATUS_TREE_NAME);
                    try {
                        final Exchange statusEx = treeService.getExchange(session, statusTreeLink);
                        deleteTableDefinitions(tables, schemaExchange, statusEx);
                    }finally {
                        treeService.releaseExchange(session, schemaExchange);
                    }
                }
            });
        } catch (PersistitException ex) {
            throw new PersistItErrorException (ex);
        }
    }
/*
    private void deleteTableDefinitions(List<TableName> tables, Exchange schemaEx, Exchange statusEx) throws Exception {
        for(final TableName tn : tables) {
            schemaEx.clear().append(BY_NAME).append(tn.getSchemaName()).append(tn.getTableName());
            final KeyFilter keyFilter = new KeyFilter(schemaEx.getKey(), 4, 4);
            schemaEx.clear();
            while (schemaEx.next(keyFilter)) {
                final int tableId = schemaEx.getKey().indexTo(-1).decodeInt();
                schemaEx.remove();
                statusEx.clear().append(tableId).remove();
                treeService.getTableStatusCache().drop(tableId);
            }
        }
    }
*/
    
    private void deleteTableDefinitions(List<TableName> tables, Exchange schemaEx, Exchange statusEx) {
        for(final TableName tn : tables) {
            UserTable table = getAis().getUserTable(tn);
            // Could have been group table name, nothing to cleanup there
            if(table != null) {
                int tableId = table.getTableId();
                try {
                    statusEx.clear().append(tableId).remove();
                } catch (PersistitException ex) {
                    throw new PersistItErrorException (ex);
                }
                // Status created on demand, can't check
                treeService.getTableStatusCache().drop(tableId);
            }
        }
    }
    
    
    @Override
    public TableDefinition getTableDefinition(Session session, TableName tableName) {
        final Table table = getAis(session).getTable(tableName);
        if(table == null) {
            throw new NoSuchTableException(tableName);
        }
        final String ddl = new DDLGenerator().createTable(table);
        return new TableDefinition(table.getTableId(), tableName, ddl);
    }

    @Override
    public SortedMap<String, TableDefinition> getTableDefinitions(Session session, String schemaName) {
        final SortedMap<String, TableDefinition> result = new TreeMap<String, TableDefinition>();
        final DDLGenerator gen = new DDLGenerator();
        for(UserTable table : getAis().getUserTables().values()) {
            final TableName name = table.getName();
            if(name.getSchemaName().equals(schemaName)) {
                final String ddl = gen.createTable(table);
                final TableDefinition def = new TableDefinition(table.getTableId(), name, ddl);
                result.put(name.getTableName(), def);
            }
        }
        return result;
    }

    @Override
    public AkibanInformationSchema getAis(Session session) {
        return getAis();
    }

    private AkibanInformationSchema getAis() {
        return aish.getAis();
    }

    private void setGroupTableIds(AkibanInformationSchema newAIS) {
       // Old behavior, reassign group table IDs
        for(GroupTable groupTable: newAIS.getGroupTables().values()) {
            final UserTable root = groupTable.getRoot();
            assert root != null : "Group with no root table: " + groupTable;
            groupTable.setTableId(TreeService.MAX_TABLES_PER_VOLUME - root.getTableId());
        }
    }

    /**
     * Construct a new AIS instance containing a copy of the currently known
     * data, see @{link #ais}, and the given schemaDef.
     * @param schemaDef SchemaDef to combine.
     * @return A completely new AIS
     * @throws Exception For any error.
     */
    private AkibanInformationSchema addSchemaDefToAIS(SchemaDef schemaDef) {
        AkibanInformationSchema newAis = new AkibanInformationSchema();
        new Writer(new AISTarget(newAis)).save(getAis());
        new SchemaDefToAis(schemaDef, newAis, true).getAis();
        setGroupTableIds(newAis);
        return newAis;
    }

    /**
     * Construct a new AIS instance containing a copy of the currently known data, see @{link #ais},
     * minus the given list of TableNames.
     * @param tableNames List of tables to exclude from new AIS.
     * @return A completely new AIS.
     * @throws Exception For any error.
     */
    private AkibanInformationSchema removeTablesFromAIS(final List<TableName> tableNames) {
        AkibanInformationSchema newAis = new AkibanInformationSchema();
        new TableSubsetWriter(new AISTarget(newAis)) {
            @Override
            public boolean shouldSaveTable(Table table) {
                return !tableNames.contains(table.getName());
            }
        }.save(getAis());

        // Fix up group table columns and indexes for modified groups
        AISBuilder builder = new AISBuilder(newAis);
        Set<String> handledGroups = new HashSet<String>();
        for(TableName tn : tableNames) {
            final UserTable oldUserTable = getAis().getUserTable(tn);
            if(oldUserTable != null) {
                final String groupName = oldUserTable.getGroup().getName();
                final Group newGroup = newAis.getGroup(groupName);
                if(newGroup != null && !handledGroups.contains(groupName)) {
                    // Since removeIndexes() removes by value, and not name, must get new instances
                    List<GroupIndex> groupIndexes = new ArrayList<GroupIndex>();
                    for(GroupIndex index : oldUserTable.getGroupIndexes()) {
                        groupIndexes.add(newGroup.getIndex(index.getIndexName().getName()));
                    }
                    newGroup.removeIndexes(groupIndexes);
                    builder.generateGroupTableColumns(newGroup);
                    builder.generateGroupTableIndexes(newGroup);
                    handledGroups.add(groupName);
                }
            }
        }
        return newAis;
    }

    @Override
    public List<String> schemaStrings(Session session, boolean withGroupTables) {
        final AkibanInformationSchema ais = getAis();
        final DDLGenerator generator = new DDLGenerator();
        final List<String> ddlList = new ArrayList<String>();
        final Set<String> sawSchemas = new HashSet<String>();
        Collection<? extends Table> tableCollection = ais.getUserTables().values();
        boolean firstPass = true;
        while(firstPass || tableCollection != null) {
            for(Table table : tableCollection) {
                final String schemaName = table.getName().getSchemaName();
                if(!sawSchemas.contains(schemaName)) {
                    final String createSchema = String.format(CREATE_SCHEMA_FORMATTER, schemaName);
                    ddlList.add(createSchema);
                    sawSchemas.add(schemaName);
                }
                final String ddl = generator.createTable(table);
                ddlList.add(ddl);
            }
            tableCollection = (firstPass && withGroupTables) ? ais.getGroupTables().values() : null;
            firstPass = false;
        }
        return ddlList;
    }

    @Override
    public long getUpdateTimestamp() {
        return updateTimestamp.get();
    }

    @Override
    public int getSchemaGeneration() {
        final long ts = getUpdateTimestamp();
        return (int) ts ^ (int) (ts >>> 32);
    }

    @Override
    public synchronized void forceNewTimestamp() {
        Session session = sessionService.createSession();
        try {
            updateTimestamp.set(treeService.getTimestamp(session));
        } finally {
            session.close();
        }
    }

    @Override
    public SchemaManager cast() {
        return this;
    }

    @Override
    public Class<SchemaManager> castClass() {
        return SchemaManager.class;
    }

    @Override
    public void start() {
        updateTimestamp = new AtomicLong();
        maxAISBufferSize = Integer.parseInt(config.getProperty(MAX_AIS_SIZE_PROPERTY));
        if(maxAISBufferSize < 0) {
            LOG.warn("Clamping property "+MAX_AIS_SIZE_PROPERTY+" to 0");
            maxAISBufferSize = 0;
        }
        // 0 = unlimited, start off at 1MB in this case.
        aisByteBuffer = ByteBuffer.allocate(maxAISBufferSize != 0 ? maxAISBufferSize : 1<<20);
        try {
            afterStart();
        } catch (PersistitException e) {
            throw new PersistItErrorException(e);
        } catch (IOException e) {
            throw new SchemaLoadIOException(e.getMessage());
        }
    }

    @Override
    public void stop() {
        this.aish.setAis(null);
        this.updateTimestamp = null;
        this.maxAISBufferSize = 0;
        this.aisByteBuffer = null;
    }

    @Override
    public void crash() {
        stop();
    }

    /**
     * Load the AIS tables from file and then load the serialized AIS from each
     * {@link TreeService#SCHEMA_TREE_NAME} in each volume. This must be done
     * in afterStart because it requires other services, TreeService and Store
     * specifically, to be up and functional
     * @throws PersistitException 
     * @throws IOException 
     */
    private void afterStart() throws PersistitException, IOException {
        final Session session = sessionService.createSession();
        final Transaction transaction = treeService.getTransaction(session);
        int retries = MAX_TRANSACTION_RETRY_COUNT;
        InputStream aisFileStream = null;
        try {
            for (;;) {
                transaction.begin();
                try {

                    // Create AIS tables
                    aisFileStream = AkServer.class.getClassLoader().getResourceAsStream(AIS_DDL_NAME);
                    SchemaDef schemaDef = SchemaDef.parseSchemaFromStream(aisFileStream);
                    final AkibanInformationSchema newAIS = new SchemaDefToAis(schemaDef, true).getAis();
                    int curId = AIS_BASE_TABLE_ID;
                    for(UserTable table : newAIS.getUserTables().values()) {
                        table.setTableId(curId++);
                        setTreeNames(table);
                    }
                    setGroupTableIds(newAIS);

                    // Load stored AIS data from each schema tree
                    treeService.visitStorage(session, new TreeVisitor() {
                                                 @Override
                                                 public void visit(Exchange ex) throws PersistitException{
                                                     ex.clear().append(BY_AIS);
                                                     if(ex.isValueDefined()) {
                                                         byte[] storedAIS = ex.fetch().getValue().getByteArray();
                                                         ByteBuffer buffer = ByteBuffer.wrap(storedAIS);
                                                         new Reader(new MessageSource(buffer)).load(newAIS);
                                                     }
                                                 }
                                             }, SCHEMA_TREE_NAME);

                    forceNewTimestamp();
                    onTransactionCommit(newAIS, updateTimestamp.get());
                    transaction.commit();

                    break; // Success

                } catch (RollbackException e) {
                    if (--retries < 0) {
                        throw new ScanRetryAbandonedException(MAX_TRANSACTION_RETRY_COUNT);
                    }
                } finally {
                    if(aisFileStream != null) {
                        aisFileStream.close();
                    }
                    transaction.end();
                }
            }
        } finally {
            session.close();
        }
    }

    private void onTransactionCommit(final AkibanInformationSchema newAis, final long timestamp) {
        // None of this code "can fail" because it is being ran inside of a Persistit commit. Fail LOUDLY.
        try {
            final RowDefCache rowDefCache = store.getRowDefCache();
            rowDefCache.clear();
            treeService.getTableStatusCache().detachAIS();
            rowDefCache.setAIS(newAis);
            updateTimestamp.set(timestamp);
            aish.setAis(newAis);
        }
        catch(RuntimeException e) {
            LOG.error(COMMIT_AIS_ERROR_MSG, e);
            throw e;
        }
        catch(Error e) {
            LOG.error(COMMIT_AIS_ERROR_MSG, e);
            throw e;
        }
    }

    private SchemaDef parseTableStatement(String defaultSchemaName, String ddl) throws InvalidOperationException {
        SchemaDef def = new SchemaDef();
        def.setMasterSchemaName(defaultSchemaName);
        try {
            def.parseCreateTable(ddl);
        } catch (SchemaDef.SchemaDefException ex) {
            throw new ParseException (defaultSchemaName, ex.getMessage(), ddl);
        }
        return def;
    }

    private void validateTableDefinition(final SchemaDef.UserTableDef tableDef) {
        final String schemaName = tableDef.getCName().getSchema();
        final String tableName = tableDef.getCName().getName();
        if (TableName.AKIBAN_INFORMATION_SCHEMA.equals(schemaName)) {
            throw new ProtectedTableDDLException(new TableName(tableName, schemaName));
        }

        final String tableCharset = tableDef.getCharset();
        if (tableCharset != null && !Charset.isSupported(tableCharset)) {
            throw new UnsupportedCharsetException(schemaName, tableName, tableCharset);
        }

        for (SchemaDef.ColumnDef col : tableDef.getColumns()) {
            final String typeName = col.getType();
            if (!getAis().isTypeSupported(typeName)) {
                throw new UnsupportedDataTypeException (
                        new TableName(schemaName, tableName),
                        col.getName(), typeName);
            }
            final String charset = col.getCharset();
            if (charset != null && !Charset.isSupported(charset)) {
                throw new UnsupportedCharsetException(schemaName, tableName, charset);
            }
        }

        for (SchemaDef.IndexColumnDef colDef : tableDef.getPrimaryKey().getColumns()) {
            final SchemaDef.ColumnDef col = tableDef.getColumn(colDef.getColumnName());
            if (col != null) {
                final String typeName = col.getType();
                if (!getAis().isTypeSupportedAsIndex(typeName)) {
                    throw new UnsupportedIndexDataTypeException (new TableName(schemaName, tableName),
                            "PRIMARY", colDef.getColumnName(), typeName);
                }
            }
        }

        for (SchemaDef.IndexDef index : tableDef.getIndexes()) {
            for (String colName : index.getColumnNames()) {
                final SchemaDef.ColumnDef col = tableDef.getColumn(colName);
                if (col != null) {
                    final String typeName = col.getType();
                    if (!getAis().isTypeSupportedAsIndex(typeName)) {
                        throw new UnsupportedIndexDataTypeException (new TableName (schemaName, tableName),
                                index.getName(), colName, typeName);
                    }
                }
            }
        }

        final List<SchemaDef.ReferenceDef> parentJoins = tableDef
                .getAkibanJoinRefs();
        if (parentJoins.isEmpty()) {
            return;
        }

        if (parentJoins.size() > 1) {
            throw new JoinToMultipleParentsException (new TableName(schemaName, tableName));
        }

        final SchemaDef.ReferenceDef parentJoin = parentJoins.get(0);
        final String parentTableName = parentJoin.getTableName();
        final String parentSchema = parentJoin.getSchemaName() != null ? parentJoin
                .getSchemaName() : schemaName;

        if (TableName.AKIBAN_INFORMATION_SCHEMA.equals(parentSchema)) {
            throw new JoinToProtectedTableException (new TableName(schemaName, tableName), new TableName(parentSchema, parentTableName));
        }

        final UserTable parentTable = getAis().getUserTable(parentSchema,
                parentTableName);
        if (schemaName.equals(parentSchema)
                && tableName.equals(parentTableName) || parentTable == null) {
            throw new JoinToUnknownTableException (new TableName (schemaName, tableName), 
                    new TableName(parentSchema, parentTableName));
        }

        List<String> childColumns = parentJoin.getIndex().getColumnNames();
        List<String> parentColumns = parentJoin.getColumns();
        List<Column> parentPKColumns = parentTable.getPrimaryKey() == null ? null
                : parentTable.getPrimaryKey().getColumns();
        if (parentColumns.size() != childColumns.size()
                || parentPKColumns == null
                || parentColumns.size() != parentPKColumns.size()) {
            
            throw new JoinColumnMismatchException (childColumns.size(), 
                   new TableName(schemaName, tableName),
                   parentTable.getName(),
                   parentColumns.size());
        }

        Iterator<String> childColumnIt = childColumns.iterator();
        Iterator<Column> parentPKIt = parentPKColumns.iterator();
        for (String parentColumnName : parentColumns) {
            // Check same columns
            String childColumnName = childColumnIt.next();
            Column parentPKColumn = parentPKIt.next();
            if (!parentColumnName.equalsIgnoreCase(parentPKColumn.getName())) {
                throw new JoinToWrongColumnsException (
                        new TableName(schemaName, tableName), parentColumnName,
                        new TableName(parentSchema, parentTableName), parentPKColumn.getName());
            }
            // Check child column exists
            SchemaDef.ColumnDef columnDef = tableDef.getColumn(childColumnName);
            if (columnDef == null) {
                throw new JoinToWrongColumnsException (
                        new TableName(schemaName, tableName), childColumnName,
                        new TableName(parentSchema, parentTableName), "UNKNOWN");
            }
            // Check child and parent column types
            final String type = columnDef.getType();
            final String parentType = parentPKColumn.getType().name();
            if (!getAis().canTypesBeJoined(parentType, type)) {
                throw new JoinToWrongColumnsException (new TableName (schemaName, tableName), columnDef.getName(),
                        new TableName (parentSchema, parentTableName), parentPKColumn.getName());
            }
        }
    }

    private long getMaxKeyStorageSize(final Column column) {
        final Type type = column.getType();
        return EncoderFactory.valueOf(type.encoding(), type).getMaxKeyStorageSize(column);
    }

    private void validateIndexSizes(final UserTable table) throws InvalidOperationException {
        final HKey hkey = table.hKey();

        long hkeySize = 0;
        int ordinalSize = 0;
        for(HKeySegment hkSeg : hkey.segments()) {
            ordinalSize += MAX_ORDINAL_STORAGE_SIZE; // one per segment (i.e. table)
            for(HKeyColumn hkCol : hkSeg.columns()) {
                hkeySize += getMaxKeyStorageSize(hkCol.column());
            }
        }

        // HKey is too large due to pk being too big or group is too nested
        if((hkeySize + ordinalSize) > MAX_INDEX_STORAGE_SIZE) {
            throw new UnsupportedIndexSizeException (table.getName(), "HKey");
        }

        // includingInternal so all indexes are checked, including (hidden) PRIMARY, since any non-hkey
        // equivalent index will a) get an index tree and b) have column(s) contributing additional size
        for(Index index : table.getIndexesIncludingInternal()) {
            long fullKeySize = hkeySize;
            for(IndexColumn iColumn : index.getColumns()) {
                final Column column = iColumn.getColumn();
                // Only indexed columns not in hkey contribute new information
                if(!hkey.containsColumn(column)) {
                    fullKeySize += getMaxKeyStorageSize((column));
                }

                // Reject prefix indexes until supported (bug760202)
                if(index.isUnique() && iColumn.getIndexedLength() != null) {
                    throw new UnsupportedIndexPrefixException (table.getName(), index.getIndexName().getName());
                }
            }
            if(fullKeySize > MAX_INDEX_STORAGE_SIZE) {
                throw new UnsupportedIndexSizeException (table.getName(), index.getIndexName().getName());
            }
        }
    }

    /**
     * Serialize the given AIS into a ByteBuffer in the MessageTarget format. Only tables existing
     * in the given volume will be written.
     * @param newAIS The AIS to serialize.
     * @param volumeName Volume to restrict tables to.
     * @return ByteBuffer
     * @throws Exception For any error during serialization or if the buffer is too small.
     */
    private ByteBuffer trySerializeAIS(final AkibanInformationSchema newAIS, final String volumeName) {
        boolean finishedSerializing = false;
        while(!finishedSerializing) {
            try {
                aisByteBuffer.clear();
                new TableSubsetWriter(new MessageTarget(aisByteBuffer)) {
                    @Override
                    public boolean shouldSaveTable(Table table) {
                        final String schemaName = table.getName().getSchemaName();
                        return !schemaName.equals(TableName.AKIBAN_INFORMATION_SCHEMA) &&
                               getVolumeForSchemaTree(schemaName).equals(volumeName);
                    }
                }.save(newAIS);
                aisByteBuffer.flip();
                finishedSerializing = true;
            }
            catch(BufferOverflowException e) {
                if(aisByteBuffer.capacity() == maxAISBufferSize) {
                    throw new AISTooLargeException (aisByteBuffer.capacity(), maxAISBufferSize);
                }
                
                int newCapacity = aisByteBuffer.capacity() * 2;
                if(maxAISBufferSize != 0 && newCapacity > maxAISBufferSize) {
                    newCapacity = maxAISBufferSize;
                }
                aisByteBuffer = ByteBuffer.allocate(newCapacity);
            }
        }
        return aisByteBuffer;
    }

    private String getVolumeForSchemaTree(final String schemaName) {
        return treeService.volumeForTree(schemaName, SCHEMA_TREE_NAME);
    }

    /**
     * Internal helper intended to be called to finalize any AIS change. This includes create, delete,
     * alter, etc. This currently updates the {@link TreeService#SCHEMA_TREE_NAME} for a given schema,
     * rebuilds the {@link Store#getRowDefCache()}, and sets the {@link #getAis()} variable.
     * @param session Session to run under
     * @param newAIS The new AIS to store in the {@link #BY_AIS} key range <b>and</b> commit as {@link #getAis()}.
     * @param schemaName The schema the change affected.
     * @param callback If non-null, beforeCommit while be called before transaction.commit().
     * @throws PersistitException 
     */
    private void commitAISChange(final Session session, final AkibanInformationSchema newAIS, final String schemaName,
                                 AISChangeCallback callback) throws PersistitException {

        //TODO: Verify the newAIS.isFrozen(), if not throw an exception. 
        ByteBuffer buffer = trySerializeAIS(newAIS, getVolumeForSchemaTree(schemaName));
        final Transaction transaction = treeService.getTransaction(session);
        int retries = MAX_TRANSACTION_RETRY_COUNT;
        for(;;) {
            final TreeLink schemaTreeLink =  treeService.treeLink(schemaName, SCHEMA_TREE_NAME);
            final Exchange schemaEx = treeService.getExchange(session, schemaTreeLink);
            try {
                transaction.begin();
                try {
                    if(callback != null) {
                        callback.beforeCommit(schemaEx, treeService);
                    }

                    schemaEx.clear().append(BY_AIS);
                    schemaEx.getValue().clear();
                    schemaEx.getValue().putByteArray(buffer.array(), buffer.position(), buffer.limit());
                    schemaEx.store();

                    transaction.commit(new DefaultCommitListener() {
                        @Override
                        public void committed() {
                            onTransactionCommit(newAIS, transaction.getCommitTimestamp());
                        }
                    }, forceToDisk);

                    break; // Success

                } catch (RollbackException e) {
                    if (--retries < 0) {
                        
                        throw new TransactionFailedException();
                    }
                } finally {
                    transaction.end();
                }
            } finally {
                treeService.releaseExchange(session, schemaEx);
            }
        }
    }
}
