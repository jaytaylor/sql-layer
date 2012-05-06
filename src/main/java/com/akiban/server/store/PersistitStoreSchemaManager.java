/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.store;

import static com.akiban.server.service.tree.TreeService.SCHEMA_TREE_NAME;

import java.nio.BufferOverflowException;
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
import com.akiban.ais.model.validation.AISValidations;
import com.akiban.ais.protobuf.ProtobufReader;
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
import com.akiban.server.error.TableNotInGroupException;
import com.akiban.server.rowdata.RowDefCache;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeLink;
import com.akiban.util.GrowableByteBuffer;
import com.google.inject.Inject;

import com.persistit.Key;
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

/**
 * <p>
 * Older, MetaModel based storage:
 * <table border="1">
 *     <tr>
 *         <th>key</th>
 *         <th>value</th>
 *         <th>description</th>
 *     </tr>
 *     <tr>
 *         <td>"byAIS"</td>
 *         <td>byte[]</td>
 *         <td>As constructed by {@link MessageTarget}</td>
 *     </tr>
 * </table>
 * </p>
 * <br>
 * <p>
 * Newer, Protobuf based storage:
 * <table border="1">
 *     <tr>
 *         <th>key</th>
 *         <th>value</th>
 *         <th>description</th>
 *     </tr>
 *     <tr>
 *         <td>"byPBAIS"</td>
 *         <td>integer</td>
 *         <td>Logical PSSM version stored, see {@link #PROTOBUF_PSSM_VERSION} for current</td>
 *     </tr>
 *     <tr>
 *         <td>"byPBAIS","schema",SCHEMA_NAME1</td>
 *         <td>byte[]</td>
 *         <td>As constructed by {@link com.akiban.ais.protobuf.ProtobufWriter}</td>
 *     </tr>
 *     <tr>
 *         <td>"byPBAIS","schema",SCHEMA_NAME2</td>
 *         <td>...</td>
 *         <td>...</td>
 *     </tr>
 *     <tr>
 *         <td>...</td>
 *         <td>...</td>
 *         <td>...</td>
 *     </tr>
 * </table>
 * </p>
 */
public class PersistitStoreSchemaManager implements Service<SchemaManager>, SchemaManager {
    private static final String METAMODEL_PARENT_KEY = "byAIS";
    private static final String PROTOBUF_PARENT_KEY = "byPBAIS";
    private static final String PROTOBUF_SCHEMA_KEY = "schema";
    private static final int PROTOBUF_PSSM_VERSION = 1;

    private static final Logger LOG = LoggerFactory.getLogger(PersistitStoreSchemaManager.class.getName());

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
    private GrowableByteBuffer aisByteBuffer;
    private boolean useMetaModelSerialization;

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
        aisByteBuffer = new GrowableByteBuffer(maxAISBufferSize != 0 ? maxAISBufferSize : 1<<20);

        try {
            loadAISFromDisk();
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
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
        /*
         * Big, ugly, and lots of hard coding. This is because any change in
         * table definition or derived data (tree name, ids, etc) affects the
         * compatibility of existing volumes. If we stopped creating this at
         * every start-up and only did it once (on fresh volume), this could
         * much shortened -- but that is only a possible TO-DO item.
         */
        final String SCHEMA = "akiban_information_schema";
        final String STATS = "zindex_statistics";
        final int STATS_ID = 1000000009;
        final String ENTRY = "zindex_statistics_entry";
        final int ENTRY_ID = 1000000010;
        final String PRIMARY = "PRIMARY";
        final String FK_NAME = "__akiban_fk_0";
        final String GROUP = STATS;
        final String GROUP_TABLE = "_akiban_" + STATS;
        final String JOIN = String.format("%s/%s/%s/%s", SCHEMA, STATS, SCHEMA, ENTRY);
        final String STATS_TREE = "akiban_information_schema$$_akiban_zindex_statistics";
        final String TREE_NAME_FORMAT = "%s$$%s$$%s$$%s$$%d";
        final String STATS_PK_TREE = String.format(TREE_NAME_FORMAT, STATS, SCHEMA, STATS, PRIMARY, 9);
        final String ENTRY_PK_TREE = String.format(TREE_NAME_FORMAT, STATS, SCHEMA, ENTRY, PRIMARY, 11);
        final String ENTRY_FK_TREE = String.format(TREE_NAME_FORMAT, STATS, SCHEMA, ENTRY, FK_NAME, 10);

        AISBuilder builder = new AISBuilder();

        int col = 0;
        builder.userTable(SCHEMA, STATS);
        builder.column(SCHEMA, STATS,           "table_id", col++,       "int", null, null, false, false, null, null);
        builder.column(SCHEMA, STATS,           "index_id", col++,       "int", null, null, false, false, null, null);
        builder.column(SCHEMA, STATS, "analysis_timestamp", col++, "timestamp", null, null,  true, false, null, null);
        builder.column(SCHEMA, STATS,          "row_count", col++,    "bigint", null, null,  true, false, null, null);
        builder.column(SCHEMA, STATS,      "sampled_count", col++,    "bigint", null, null,  true, false, null, null);
        col = 0;
        builder.index(SCHEMA, STATS, PRIMARY, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(SCHEMA, STATS, PRIMARY, "table_id", col++, true, null);
        builder.indexColumn(SCHEMA, STATS, PRIMARY, "index_id", col++, true, null);

        col = 0;
        builder.userTable(SCHEMA, ENTRY);
        builder.column(SCHEMA, ENTRY,       "table_id", col++,       "int",  null, null, false, false, null, null);
        builder.column(SCHEMA, ENTRY,       "index_id", col++,       "int",  null, null, false, false, null, null);
        builder.column(SCHEMA, ENTRY,   "column_count", col++,       "int",  null, null, false, false, null, null);
        builder.column(SCHEMA, ENTRY,    "item_number", col++,       "int",  null, null, false, false, null, null);
        builder.column(SCHEMA, ENTRY,     "key_string", col++,   "varchar", 2048L, null,  true, false, null, null);
        builder.column(SCHEMA, ENTRY,      "key_bytes", col++, "varbinary", 4096L, null,  true, false, null, null);
        builder.column(SCHEMA, ENTRY,       "eq_count", col++,    "bigint",  null, null,  true, false, null, null);
        builder.column(SCHEMA, ENTRY,       "lt_count", col++,    "bigint",  null, null,  true, false, null, null);
        builder.column(SCHEMA, ENTRY, "distinct_count", col++,    "bigint",  null, null,  true, false, null, null);
        col = 0;
        builder.index(SCHEMA, ENTRY, PRIMARY, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(SCHEMA, ENTRY, PRIMARY,     "table_id", col++, true, null);
        builder.indexColumn(SCHEMA, ENTRY, PRIMARY,     "index_id", col++, true, null);
        builder.indexColumn(SCHEMA, ENTRY, PRIMARY, "column_count", col++, true, null);
        builder.indexColumn(SCHEMA, ENTRY, PRIMARY,  "item_number", col++, true, null);
        col = 0;
        builder.index(SCHEMA, ENTRY, FK_NAME, false, "FOREIGN KEY");
        builder.indexColumn(SCHEMA, ENTRY, FK_NAME, "table_id", col++, true, null);
        builder.indexColumn(SCHEMA, ENTRY, FK_NAME, "index_id", col++, true, null);

        builder.joinTables(JOIN, SCHEMA, STATS, SCHEMA, ENTRY);
        builder.joinColumns(JOIN, SCHEMA, STATS, "table_id", SCHEMA, ENTRY, "table_id");
        builder.joinColumns(JOIN, SCHEMA, STATS, "index_id", SCHEMA, ENTRY, "index_id");

        builder.createGroup(GROUP, SCHEMA, GROUP_TABLE);
        builder.addJoinToGroup(GROUP, JOIN, 0);

        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();

        AkibanInformationSchema primordialAIS = builder.akibanInformationSchema();
        
        UserTable statsTable = primordialAIS.getUserTable(SCHEMA, STATS);
        statsTable.setTableId(STATS_ID);
        statsTable.setTreeName(STATS_TREE);
        statsTable.getIndex(PRIMARY).setTreeName(STATS_PK_TREE);
        
        UserTable entryTable = primordialAIS.getUserTable(SCHEMA, ENTRY);
        entryTable.setTableId(ENTRY_ID);
        entryTable.setTreeName(STATS_TREE);
        entryTable.getIndex(PRIMARY).setTreeName(ENTRY_PK_TREE);
        entryTable.getIndex(FK_NAME).setTreeName(ENTRY_FK_TREE);

        primordialAIS.validate(AISValidations.LIVE_AIS_VALIDATIONS);

        return primordialAIS;
    }

    /**
     * Load the AIS tables from file by iterating every volume and reading the contents
     * of the {@link TreeService#SCHEMA_TREE_NAME} tree.
     * @throws PersistitException
     */
    private void loadAISFromDisk() throws PersistitException {
        final AkibanInformationSchema newAIS = createPrimordialAIS();
        setGroupTableIds(newAIS);

        final Session session = sessionService.createSession();
        final Transaction transaction = treeService.getTransaction(session);
        transaction.begin();
        try {
            treeService.visitStorage(session, new TreeVisitor() {
                @Override
                public void visit(Exchange ex) throws PersistitException{
                    // Simple heuristic to determine which style AIS storage we have
                    // TODO: This is where the "automatic upgrade" would go

                    boolean hasMetaModel = ex.clear().append(METAMODEL_PARENT_KEY).isValueDefined();
                    boolean hasProtobuf = ex.clear().append(PROTOBUF_PARENT_KEY).isValueDefined();

                    if(hasMetaModel && hasProtobuf) {
                        throw new IllegalStateException("Both AIS and Protobuf serializations");
                    }

                    if(hasMetaModel) {
                        useMetaModelSerialization = true;
                        loadMetaModelBasedAIS(ex, newAIS);
                    } else if(hasProtobuf) {
                        if(useMetaModelSerialization) {
                            throw new IllegalStateException("Mixed AIS and Protobuf serializations: " + ex);
                        }
                        loadProtobufBasedAIS(ex, newAIS);
                    } else {
                        throw new IllegalStateException("No (or unknown) AIS serialization");
                    }
                }
            }, SCHEMA_TREE_NAME);

            buildRowDefCache(newAIS);
            transaction.commit();
        } finally {
            transaction.end();
            session.close();
        }
    }

    private static void loadMetaModelBasedAIS(Exchange ex, AkibanInformationSchema newAIS) throws PersistitException {
        ex.clear().append(METAMODEL_PARENT_KEY).fetch();
        if(!ex.getValue().isDefined()) {
            throw new IllegalStateException(ex.toString() + " has no associated value (expected byte[])");
        }
        byte[] storedAIS = ex.getValue().getByteArray();
        GrowableByteBuffer buffer = GrowableByteBuffer.wrap(storedAIS);
        Reader reader = new Reader(new MessageSource(buffer));
        reader.load(newAIS);
    }

    private static void loadProtobufBasedAIS(Exchange ex, AkibanInformationSchema newAIS) throws PersistitException {
        ex.clear().append(PROTOBUF_PARENT_KEY).fetch();
        if(!ex.getValue().isDefined()) {
            throw new IllegalStateException(ex.toString() + " has no associated value (expected int)");
        }

        int storedVersion = ex.getValue().getInt();
        if(storedVersion != PROTOBUF_PSSM_VERSION) {
            throw new IllegalStateException("Unexpected Protobuf PSSM version: " + storedVersion);
        }

        ex.append(PROTOBUF_SCHEMA_KEY).append(Key.BEFORE);
        while(ex.next()) {
            byte[] storedAIS = ex.getValue().getByteArray();
            GrowableByteBuffer buffer = GrowableByteBuffer.wrap(storedAIS);
            ProtobufReader reader = new ProtobufReader(buffer, newAIS);
            reader.load();
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
    private GrowableByteBuffer trySerializeAIS(final AkibanInformationSchema newAIS, final String volumeName) {
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
                aisByteBuffer = new GrowableByteBuffer(newCapacity);
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
     * @param newAIS The new AIS to store in the {@link #METAMODEL_PARENT_KEY} key range <b>and</b> commit as {@link #getAis()}.
     * @param schemaName The schema the change affected.
     * @throws PersistitException
     */
    private void commitAISChange(final Session session, final AkibanInformationSchema newAIS, final String schemaName)
            throws PersistitException {

        //TODO: Verify the newAIS.isFrozen(), if not throw an exception. 
        GrowableByteBuffer buffer = trySerializeAIS(newAIS, getVolumeForSchemaTree(schemaName));
        final TreeLink schemaTreeLink =  treeService.treeLink(schemaName, SCHEMA_TREE_NAME);
        final Exchange schemaEx = treeService.getExchange(session, schemaTreeLink);
        
        try {
            schemaEx.clear().append(METAMODEL_PARENT_KEY);
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
