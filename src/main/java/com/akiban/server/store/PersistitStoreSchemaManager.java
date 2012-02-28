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

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.akiban.ais.metamodel.io.AISTarget;
import com.akiban.ais.metamodel.io.MessageSource;
import com.akiban.ais.metamodel.io.MessageTarget;
import com.akiban.ais.metamodel.io.Reader;
import com.akiban.ais.metamodel.io.TableSubsetWriter;
import com.akiban.ais.metamodel.io.Writer;
import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AISMerge;
import com.akiban.ais.model.AISTableNameChanger;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.server.error.AISTooLargeException;
import com.akiban.server.error.BranchingGroupIndexException;
import com.akiban.server.error.DuplicateIndexException;
import com.akiban.server.error.DuplicateTableNameException;
import com.akiban.server.error.IndexLacksColumnsException;
import com.akiban.server.error.JoinColumnTypesMismatchException;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchGroupException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.error.ProtectedIndexException;
import com.akiban.server.error.ProtectedTableDDLException;
import com.akiban.server.error.ReferencedTableException;
import com.akiban.server.error.SchemaLoadIOException;
import com.akiban.server.error.TableNotInGroupException;
import com.akiban.server.rowdata.RowDefCache;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeLink;
import com.google.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.server.service.Service;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.service.tree.TreeVisitor;
import com.persistit.Exchange;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;

public class PersistitStoreSchemaManager implements Service<SchemaManager>,
        SchemaManager {

    final static int AIS_BASE_TABLE_ID = 1000000000;

    static final String BY_AIS = "byAIS";

    private static final Logger LOG = LoggerFactory
            .getLogger(PersistitStoreSchemaManager.class.getName());

    private final static String CREATE_SCHEMA_FORMATTER = "create schema if not exists `%s`;";

    public static final String MAX_AIS_SIZE_PROPERTY = "akserver.max_ais_size_bytes";

    private static final String TREE_NAME_SEPARATOR = "$$";

    private static final AtomicInteger indexCounter = new AtomicInteger(0);

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
            if(!index.getKeyColumns().isEmpty()) {
                Column groupColumn = index.getKeyColumns().get(0).getColumn();
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
        setTreeNames(finalTable);
        
        try {
            commitAISChange(session, merge.getAIS(), schemaName);
        } catch (PersistitException ex) {
            throw new PersistitAdapterException(ex);
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
            commitAISChange(session, newAIS, currentName.getSchemaName());
            if(!vol1.equals(vol2)) {
                commitAISChange(session, newAIS, newName.getSchemaName());
            }
        } catch (PersistitException ex) {
            throw new PersistitAdapterException(ex);
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
    
    public static Collection<Index> createIndexes(AkibanInformationSchema newAIS,
                                                  Collection<? extends Index> indexesToAdd) {
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

            switch(index.getIndexType()) {
                case TABLE:
                {
                    final TableName tableName = new TableName(indexName.getSchemaName(), indexName.getTableName());
                    final UserTable newTable = newAIS.getUserTable(tableName);
                    if(newTable == null) {
                        throw new NoSuchTableException(tableName);
                    }
                    curIndex = newTable.getIndex(indexName.getName());
                    newGroup = newTable.getGroup();
                    Integer newId = findMaxIndexIDInGroup(newAIS, newGroup) + 1;
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
                    Integer newId = findMaxIndexIDInGroup(newAIS, newGroup) + 1;
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
            if(index.getKeyColumns().isEmpty()) {
                throw new IndexLacksColumnsException (
                        new TableName(index.getIndexName().getSchemaName(), index.getIndexName().getTableName()),
                        index.getIndexName().getName());
            }

            UserTable lastTable = null;
            for(IndexColumn indexCol : index.getKeyColumns()) {
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
        return newIndexes;
    }
    
    @Override
    public Collection<Index> createIndexes(Session session, Collection<? extends Index> indexesToAdd) {
        final Map<String,String> volumeToSchema = new HashMap<String,String>();
        final AkibanInformationSchema newAIS = new AkibanInformationSchema();
        new Writer(new AISTarget(newAIS)).save(getAis());

        Collection<Index> newIndexes = createIndexes(newAIS, indexesToAdd);
        for(Index index : newIndexes) {
            String schemaName = index.getIndexName().getSchemaName();
            volumeToSchema.put(getVolumeForSchemaTree(schemaName), schemaName);
        }
        
        for(String schema : volumeToSchema.values()) {
            try {
                commitAISChange(session, newAIS, schema);
            } catch (PersistitException ex) {
                throw new PersistitAdapterException(ex);
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
                commitAISChange(session, newAIS, schema);
            } catch (PersistitException ex) {
                throw new PersistitAdapterException(ex);
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
            commitAISChange(session, newAIS, schemaName);
            // Success, remaining cleanup
            deleteTableStatuses(tables);
        } catch (PersistitException ex) {
            throw new PersistitAdapterException(ex);
        }
    }

    private void deleteTableStatuses(List<TableName> tables) throws PersistitException {
        for(final TableName tn : tables) {
            UserTable table = getAis().getUserTable(tn);
            if(table != null) {
                treeService.getTableStatusCache().drop(table.getTableId());
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
     * Construct a new AIS instance containing a copy of the currently known data, see @{link #ais},
     * minus the given list of TableNames.
     * @param tableNames List of tables to exclude from new AIS.
     * @return A completely new AIS.
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
        long ts = getUpdateTimestamp();
        return (int) ts ^ (int) (ts >>> 32);
    }
    
    @Override
    public void forceNewTimestamp() {
        updateTimestamp.set(treeService.getDb().getCurrentTimestamp());
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
            throw new PersistitAdapterException(e);
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

    private static AkibanInformationSchema createPrimordialAIS() {
        NewAISBuilder builder = AISBBasedBuilder.create("akiban_information_schema");

        builder.userTable("zindex_statistics").
                colLong("table_id", false).
                colLong("index_id", false).
                colTimestamp("analysis_timestamp", true).
                colBigInt("row_count", true).
                colBigInt("sampled_count", true).
                pk("table_id", "index_id");

        builder.userTable("zindex_statistics_entry").
                colLong("table_id", false).
                colLong("index_id", false).
                colLong("column_count", false).
                colLong("item_number", false).
                colString("key_string", 2048, true, "latin1").
                colBinary("key_bytes", 4096, true).
                colBigInt("eq_count", true).
                colBigInt("lt_count", true).
                colBigInt("distinct_count", true).
                pk("table_id", "index_id", "column_count", "item_number").
                joinTo("zindex_statistics").on("table_id", "table_id").and("index_id", "index_id");
        
        return builder.ais(false);
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
        InputStream aisFileStream = null;
        final Session session = sessionService.createSession();
        final Transaction transaction = treeService.getTransaction(session);
        transaction.begin();
        try {
            final AkibanInformationSchema newAIS = createPrimordialAIS();
            // Offset so index stats tables get same IDs
            int curId = AIS_BASE_TABLE_ID + 9;
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

            buildRowDefCache(newAIS);
            transaction.commit();
        } finally {
            if(aisFileStream != null) {
                aisFileStream.close();
            }
            if(!transaction.isCommitted()) {
                transaction.rollback();
            }
            transaction.end();
            session.close();
        }
    }

    private void buildRowDefCache(final AkibanInformationSchema newAis) throws PersistitException {
        final RowDefCache rowDefCache = store.getRowDefCache();
        rowDefCache.clear();
        treeService.getTableStatusCache().detachAIS();
        rowDefCache.setAIS(newAis);
        forceNewTimestamp();
        aish.setAis(newAis);
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
     * @throws PersistitException
     */
    private void commitAISChange(final Session session, final AkibanInformationSchema newAIS, final String schemaName)
            throws PersistitException {

        //TODO: Verify the newAIS.isFrozen(), if not throw an exception. 
        ByteBuffer buffer = trySerializeAIS(newAIS, getVolumeForSchemaTree(schemaName));
        final TreeLink schemaTreeLink =  treeService.treeLink(schemaName, SCHEMA_TREE_NAME);
        final Exchange schemaEx = treeService.getExchange(session, schemaTreeLink);
        
        try {
            schemaEx.clear().append(BY_AIS);
            schemaEx.getValue().clear();
            schemaEx.getValue().putByteArray(buffer.array(), buffer.position(), buffer.limit());
            schemaEx.store();

            try {
                buildRowDefCache(newAIS);
            }
            catch (PersistitException e) {
                LOG.error("AIS change successful and stored on disk but RowDefCache creation failed!");
                LOG.error("RUNNING STATE NOW INCONSISTENT");
                throw e;
            }
        } finally {
            treeService.releaseExchange(session, schemaEx);
        }
    }

    /**
     * Find the maximum index ID from all of the indexes within the given group.
     */
    private static int findMaxIndexIDInGroup(AkibanInformationSchema ais, Group group) {
        int maxId = Integer.MIN_VALUE;
        for(UserTable table : ais.getUserTables().values()) {
            if(table.getGroup().equals(group)) {
                for(Index index : table.getIndexesIncludingInternal()) {
                    maxId = Math.max(index.getIndexId(), maxId);
                }
            }
        }
        for(Index index : group.getIndexes()) {
            maxId = Math.max(index.getIndexId(), maxId);
        }
        return maxId;
    }
}
