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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
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
import com.akiban.ais.model.DefaultNameGenerator;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.NameGenerator;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.validation.AISValidations;
import com.akiban.ais.protobuf.ProtobufReader;
import com.akiban.ais.protobuf.ProtobufWriter;
import com.akiban.qp.operator.memoryadapter.MemoryTableFactory;
import com.akiban.server.error.AISTooLargeException;
import com.akiban.server.error.BranchingGroupIndexException;
import com.akiban.server.error.DuplicateIndexException;
import com.akiban.server.error.DuplicateTableNameException;
import com.akiban.server.error.ISTableVersionMismatchException;
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
 * Storage as of v1.2.1 (05/2012), Protobuf bytes per schema:
 * <table border="1">
 *     <tr>
 *         <th>key</th>
 *         <th>value</th>
 *         <th>description</th>
 *     </tr>
 *     <tr>
 *         <td>"byPBAIS",(int)version,"schema1"</td>
 *         <td>byte[]</td>
 *         <td>Key is a pair of version (int, see {@link #PROTOBUF_PSSM_VERSION} for current) and schema name (string).<br>
 *             Value is as constructed by {@link ProtobufWriter}.
 *     </tr>
 *     <tr>
 *         <td>"byPBAIS",(int)version,"schema2"</td>
 *         <td>...</td>
 *         <td>...</td>
 *     </tr>
 * </table>
 * <br>
 * <p>
 * Storage as of v0.4 (05/2011), MetaModel bytes per AIS:
 * <table border="1">
 *     <tr>
 *         <th>key</th>
 *         <th>value</th>
 *         <th>description</th>
 *     </tr>
 *     <tr>
 *         <td>"byAIS"</td>
 *         <td>byte[]</td>
 *         <td>Value is as constructed by {@link MessageTarget}</td>
 *     </tr>
 * </table>
 * </p>
 * </p>
 */
public class PersistitStoreSchemaManager implements Service<SchemaManager>, SchemaManager {
    public static enum SerializationType {
        NONE,
        META_MODEL,
        PROTOBUF,
        UNKNOWN
    }

    public static final String MAX_AIS_SIZE_PROPERTY = "akserver.max_ais_size_bytes";
    public static final SerializationType DEFAULT_SERIALIZATION = SerializationType.PROTOBUF;

    private static final String METAMODEL_PARENT_KEY = "byAIS";
    private static final String PROTOBUF_PARENT_KEY = "byPBAIS";
    private static final int PROTOBUF_PSSM_VERSION = 1;

    private static final String CREATE_SCHEMA_FORMATTER = "create schema if not exists `%s`;";
    private static final Logger LOG = LoggerFactory.getLogger(PersistitStoreSchemaManager.class.getName());

    private final AisHolder aish;
    private final SessionService sessionService;
    private final Store store;
    private final TreeService treeService;
    private final ConfigurationService config;
    private AtomicLong updateTimestamp;
    private int maxAISBufferSize;
    private SerializationType serializationType = SerializationType.NONE;
    private final Set<TableName> legacyISTables = new HashSet<TableName>();

    @Inject
    public PersistitStoreSchemaManager(AisHolder aisHolder, ConfigurationService config, SessionService sessionService, Store store, TreeService treeService) {
        this.aish = aisHolder;
        this.config = config;
        this.sessionService = sessionService;
        this.treeService = treeService;
        this.store = store;
    }

    @Override
    public TableName registerStoredInformationSchemaTable(Session session, UserTable newTable, int version) {
        final TableName newName = newTable.getName();
        checkAISSchema(newName, true);
        UserTable curTable = getAis().getUserTable(newName);
        if(curTable != null) {
            Integer oldVersion = curTable.getVersion();
            if(Integer.valueOf(version).equals(oldVersion)) {
                return newName;
            } else {
                throw new ISTableVersionMismatchException(oldVersion, version);
            }
        }
        return createTableCommon(session, newTable, true, version, null);
    }

    @Override
    public TableName registerMemoryInformationSchemaTable(Session session, UserTable newTable, MemoryTableFactory factory) {
        if(factory == null) {
            throw new IllegalArgumentException("MemoryTableFactory may not be null");
        }
        return createTableCommon(session, newTable, true, null, factory);
    }

    @Override
    public TableName createTableDefinition(Session session, UserTable newTable) {
        return createTableCommon(session, newTable, false, null, null);
    }

    @Override
    public void renameTable(Session session, TableName currentName, TableName newName) {
        checkTableName(currentName, true, false);
        checkTableName(newName, false, false);

        final AkibanInformationSchema newAIS = copyAIS(getAis());
        final UserTable newTable = newAIS.getUserTable(currentName);
        
        AISTableNameChanger nameChanger = new AISTableNameChanger(newTable);
        nameChanger.setSchemaName(newName.getSchemaName());
        nameChanger.setNewTableName(newName.getTableName());
        nameChanger.doChange();

        // AISTableNameChanger doesn't bother with group names or group tables, fix them with the builder
        AISBuilder builder = new AISBuilder(newAIS);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        newAIS.freeze();

        final String curSchema = currentName.getSchemaName();
        final String newSchema = newName.getSchemaName();
        if(curSchema.equals(newSchema)) {
            commitAISChange(session, newAIS, Collections.singleton(curSchema), true);
        } else {
            commitAISChange(session, newAIS, Arrays.asList(curSchema, newSchema), true);
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
        final NameGenerator nameGen = new DefaultNameGenerator().setDefaultTreeNames(AISMerge.computeTreeNames(newAIS));

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
            newIndex.setTreeName(nameGen.generateIndexTreeName(newIndex));
            newIndexes.add(newIndex);
            builder.generateGroupTableIndexes(newGroup);
        }
        return newIndexes;
    }
    
    @Override
    public Collection<Index> createIndexes(Session session, Collection<? extends Index> indexesToAdd) {
        final Set<String> schemas = new HashSet<String>();
        final AkibanInformationSchema newAIS = copyAIS(getAis());

        Collection<Index> newIndexes = createIndexes(newAIS, indexesToAdd);
        for(Index index : newIndexes) {
            schemas.add(DefaultNameGenerator.schemaNameForIndex(index));
        }
        
        commitAISChange(session, newAIS, schemas, true);
        return newIndexes;
    }

    @Override
    public void dropIndexes(Session session, Collection<Index> indexesToDrop) {
        final AkibanInformationSchema newAIS = copyAIS(getAis());
        final AISBuilder builder = new AISBuilder(newAIS);
        final Set<String> schemas = new HashSet<String>();

        for(Index index : indexesToDrop) {
            final IndexName name = index.getIndexName();
            switch(index.getIndexType()) {
                case TABLE:
                    Table newTable = newAIS.getUserTable(new TableName(name.getSchemaName(), name.getTableName()));
                    if(newTable != null) {
                        newTable.removeIndexes(Collections.singleton(newTable.getIndex(name.getName())));
                        builder.generateGroupTableIndexes(newTable.getGroup());
                    }
                break;
                case GROUP:
                    Group newGroup = newAIS.getGroup(name.getTableName());
                    if(newGroup != null) {
                        newGroup.removeIndexes(Collections.singleton(newGroup.getIndex(name.getName())));
                    }
                break;
                default:
                    throw new IllegalArgumentException("Unknown index type: " + index);
            }

            schemas.add(DefaultNameGenerator.schemaNameForIndex(index));
        }

        commitAISChange(session, newAIS, schemas, true);
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

        final Set<String> schemas = new HashSet<String>();
        final List<Integer> tableIDs = new ArrayList<Integer>();
        for(TableName name : tables) {
            schemas.add(name.getSchemaName());
            tableIDs.add(getAis().getTable(name).getTableId());
        }

        final AkibanInformationSchema newAIS = removeTablesFromAIS(tables);
        try {
            commitAISChange(session, newAIS, schemas, true);
            // Success, remaining cleanup
            deleteTableStatuses(tableIDs);
        } catch (PersistitException ex) {
            throw new PersistitAdapterException(ex);
        }
    }

    private void deleteTableStatuses(List<Integer> tableIDs) throws PersistitException {
        for(Integer id : tableIDs) {
            treeService.getTableStatusCache().drop(id);
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

    /**
     * Construct a new AIS instance containing a copy of the currently known data, see @{link #ais},
     * minus the given list of TableNames.
     * @param tableNames List of tables to exclude from new AIS.
     * @return A completely new AIS.
     */
    private AkibanInformationSchema removeTablesFromAIS(final List<TableName> tableNames) {
        AkibanInformationSchema newAis = new AkibanInformationSchema();
        copyAIS(newAis,
                getAis(),
                new TableSubsetWriter(new AISTarget(newAis)) {
                    @Override
                    public boolean shouldSaveTable(Table table) {
                        return !tableNames.contains(table.getName());
                    }
                }
        );

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

        try {
            AkibanInformationSchema newAIS = loadAISFromStorage();
            performUpgrade(newAIS);

            final Session session = sessionService.createSession();
            final Transaction transaction = treeService.getTransaction(session);
            transaction.begin();
            try {
                buildRowDefCache(newAIS);
                transaction.commit();
            } finally {
                transaction.end();
                session.close();
            }
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    @Override
    public void stop() {
        this.aish.setAis(null);
        this.updateTimestamp = null;
        this.maxAISBufferSize = 0;
        this.serializationType = SerializationType.NONE;
        this.legacyISTables.clear();
    }

    @Override
    public void crash() {
        stop();
    }

    private static void createPrimordialTables(AkibanInformationSchema ais, String schema, String namePrefix) {
        /*
         * Big, ugly, and lots of hard coding. This is because any change in
         * table definition or derived data (tree name, ids, etc) affects the
         * compatibility of existing volumes. If we stopped creating this at
         * every start-up and only did it once (on fresh volume), this could
         * much shortened -- but that is only a possible TO-DO item.
         */
        final String TREE_SCHEMA = "akiban_information_schema";
        final String TREE_STATS = "zindex_statistics";
        final String TREE_ENTRY = TREE_STATS + "_entry";
        final String STATS = namePrefix + "index_statistics";
        final int STATS_ID = 1000000009;
        final String ENTRY = namePrefix + "index_statistics_entry";
        final int ENTRY_ID = 1000000010;
        final String PRIMARY = "PRIMARY";
        final String FK_NAME = "__akiban_fk_0";
        final String GROUP_TABLE = "_akiban_" + STATS;
        final String JOIN = String.format("%s/%s/%s/%s", schema, STATS, schema, ENTRY);
        final String STATS_TREE = "akiban_information_schema$$_akiban_zindex_statistics";
        final String TREE_NAME_FORMAT = "%s$$%s$$%s$$%s$$%d";
        final String STATS_PK_TREE = String.format(TREE_NAME_FORMAT, TREE_STATS, TREE_SCHEMA, TREE_STATS, PRIMARY, 9);
        final String ENTRY_PK_TREE = String.format(TREE_NAME_FORMAT, TREE_STATS, TREE_SCHEMA, TREE_ENTRY, PRIMARY, 11);
        final String ENTRY_FK_TREE = String.format(TREE_NAME_FORMAT, TREE_STATS, TREE_SCHEMA, TREE_ENTRY, FK_NAME, 10);

        AISBuilder builder = new AISBuilder(ais);

        int col = 0;
        builder.userTable(schema, STATS);
        builder.column(schema, STATS,           "table_id", col++,       "int", null, null, false, false, null, null);
        builder.column(schema, STATS,           "index_id", col++,       "int", null, null, false, false, null, null);
        builder.column(schema, STATS, "analysis_timestamp", col++, "timestamp", null, null,  true, false, null, null);
        builder.column(schema, STATS,          "row_count", col++,    "bigint", null, null,  true, false, null, null);
        builder.column(schema, STATS,      "sampled_count", col,      "bigint", null, null,  true, false, null, null);
        col = 0;
        builder.index(schema, STATS, PRIMARY, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(schema, STATS, PRIMARY, "table_id", col++, true, null);
        builder.indexColumn(schema, STATS, PRIMARY, "index_id", col, true, null);

        col = 0;
        builder.userTable(schema, ENTRY);
        builder.column(schema, ENTRY,       "table_id", col++,       "int",  null, null, false, false, null, null);
        builder.column(schema, ENTRY,       "index_id", col++,       "int",  null, null, false, false, null, null);
        builder.column(schema, ENTRY,   "column_count", col++,       "int",  null, null, false, false, null, null);
        builder.column(schema, ENTRY,    "item_number", col++,       "int",  null, null, false, false, null, null);
        builder.column(schema, ENTRY,     "key_string", col++,   "varchar", 2048L, null,  true, false, null, null);
        builder.column(schema, ENTRY,      "key_bytes", col++, "varbinary", 4096L, null,  true, false, null, null);
        builder.column(schema, ENTRY,       "eq_count", col++,    "bigint",  null, null,  true, false, null, null);
        builder.column(schema, ENTRY,       "lt_count", col++,    "bigint",  null, null,  true, false, null, null);
        builder.column(schema, ENTRY, "distinct_count", col,      "bigint",  null, null,  true, false, null, null);
        col = 0;
        builder.index(schema, ENTRY, PRIMARY, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(schema, ENTRY, PRIMARY,     "table_id", col++, true, null);
        builder.indexColumn(schema, ENTRY, PRIMARY,     "index_id", col++, true, null);
        builder.indexColumn(schema, ENTRY, PRIMARY, "column_count", col++, true, null);
        builder.indexColumn(schema, ENTRY, PRIMARY,  "item_number", col, true, null);
        col = 0;
        builder.index(schema, ENTRY, FK_NAME, false, "FOREIGN KEY");
        builder.indexColumn(schema, ENTRY, FK_NAME, "table_id", col++, true, null);
        builder.indexColumn(schema, ENTRY, FK_NAME, "index_id", col,   true, null);

        builder.joinTables(JOIN, schema, STATS, schema, ENTRY);
        builder.joinColumns(JOIN, schema, STATS, "table_id", schema, ENTRY, "table_id");
        builder.joinColumns(JOIN, schema, STATS, "index_id", schema, ENTRY, "index_id");

        builder.createGroup(STATS, schema, GROUP_TABLE);
        builder.addJoinToGroup(STATS, JOIN, 0);

        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();

        UserTable statsTable = ais.getUserTable(schema, STATS);
        statsTable.getGroup().getGroupTable().setTreeName(STATS_TREE);
        statsTable.setTableId(STATS_ID);
        statsTable.setTreeName(STATS_TREE);
        statsTable.getIndex(PRIMARY).setTreeName(STATS_PK_TREE);
        statsTable.setVersion(1);

        UserTable entryTable = ais.getUserTable(schema, ENTRY);
        entryTable.setTableId(ENTRY_ID);
        entryTable.setTreeName(STATS_TREE);
        entryTable.getIndex(PRIMARY).setTreeName(ENTRY_PK_TREE);
        entryTable.getIndex(FK_NAME).setTreeName(ENTRY_FK_TREE);
        entryTable.setVersion(1);

        // Legacy behavior for group table ID
        GroupTable statsGroupTable = ais.getGroupTable(schema, GROUP_TABLE);
        UserTable rootTable = statsGroupTable.getRoot();
        assert rootTable == statsTable : "Unexpected root: " + rootTable;
        statsGroupTable.setTableId(TreeService.MAX_TABLES_PER_VOLUME - rootTable.getTableId());

        ais.validate(AISValidations.LIVE_AIS_VALIDATIONS);
    }

    /**
     * Primordial AIS as required to be externally compatible (columns, etc)
     * with the legacy stats tables. This is used when an upgrade is performed
     * from MetaModel to Protobuf. After this, it is then saved out to disk and
     * is compatible with the registered table from IndexStatisticsService.
     */
    private static void createUpgradedPrimordialTables(AkibanInformationSchema ais) {
        createPrimordialTables(ais, TableName.AKIBAN_INFORMATION_SCHEMA, "");
    }

    /**
     * Primordial AIS as it has existed since the zindex* tables were added (pre 1.0).
     * This can go away when we stop supporting loading of MetaModel based AIS.
     */
    private static void createLegacyPrimordialTables(AkibanInformationSchema ais) {
        createPrimordialTables(ais, "akiban_information_schema", "z");
    }

    /**
     * Load the AIS tables from file by iterating every volume and reading the contents
     * of the {@link TreeService#SCHEMA_TREE_NAME} tree.
     */
    private AkibanInformationSchema loadAISFromStorage() throws PersistitException {
        final AkibanInformationSchema newAIS = new AkibanInformationSchema();

        final Session session = sessionService.createSession();
        final Transaction transaction = treeService.getTransaction(session);
        transaction.begin();
        try {
            treeService.visitStorage(session, new TreeVisitor() {
                @Override
                public void visit(Exchange ex) throws PersistitException{
                    SerializationType typeForVolume = detectSerializationType(ex);
                    switch(typeForVolume) {
                        case NONE:
                            // Empty tree, nothing to do
                        break;
                        case META_MODEL:
                            checkAndSetSerialization(typeForVolume);
                            loadMetaModel(ex, newAIS);
                        break;
                        case PROTOBUF:
                            checkAndSetSerialization(typeForVolume);
                            loadProtobuf(ex, newAIS);
                        break;
                        case UNKNOWN:
                            throw new IllegalStateException("Unknown AIS serialization: " + ex);
                        default:
                            throw new IllegalStateException("Unhandled serialization type: " + typeForVolume);
                    }
                }
            }, SCHEMA_TREE_NAME);

            transaction.commit();
        } finally {
            transaction.end();
            session.close();
        }

        return newAIS;
    }

    private static void loadMetaModel(Exchange ex, AkibanInformationSchema newAIS) throws PersistitException {
        ex.clear().append(METAMODEL_PARENT_KEY).fetch();
        if(!ex.getValue().isDefined()) {
            throw new IllegalStateException(ex.toString() + " has no associated value (expected byte[])");
        }
        byte[] storedAIS = ex.getValue().getByteArray();
        GrowableByteBuffer buffer = GrowableByteBuffer.wrap(storedAIS);
        Reader reader = new Reader(new MessageSource(buffer));
        reader.load(newAIS);
    }

    private static void loadProtobuf(Exchange ex, AkibanInformationSchema newAIS) throws PersistitException {
        ProtobufReader reader = new ProtobufReader(newAIS);
        Key key = ex.getKey();
        key.clear().append(PROTOBUF_PARENT_KEY).append(Key.BEFORE);
        while(ex.next(true)) {
            if(key.getDepth() != 3) {
                throw new IllegalStateException("Unexpected "+PROTOBUF_PARENT_KEY+" format: " + key);
            }

            key.indexTo(1);
            int storedVersion = key.decodeInt();
            String storedSchema = key.decodeString();
            if(storedVersion != PROTOBUF_PSSM_VERSION) {
                throw new IllegalArgumentException("Unsupported version for schema "+storedSchema+": " + storedVersion);
            }

            byte[] storedAIS = ex.getValue().getByteArray();
            GrowableByteBuffer buffer = GrowableByteBuffer.wrap(storedAIS);
            reader.loadBuffer(buffer);
        }

        reader.loadAIS();

        // ProtobufWriter does not save group tables (by design) so generate columns and indexes
        AISBuilder builder = new AISBuilder(newAIS);
        builder.groupingIsComplete();
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
     * Serialize the given AIS into a ByteBuffer, either appropriate format according to the
     * {@link #serializationType} variable. An exception will be thrown in the required
     * size exceeds the {@link #maxAISBufferSize} setting.
     *
     * @param newAIS The AIS to serialize.
     * @param schemaName Schema to restrict tables to.
     */
    private void saveAISToStorage(Exchange ex, AkibanInformationSchema newAIS, String schemaName) throws PersistitException {
        int maxSize = maxAISBufferSize == 0 ? Integer.MAX_VALUE : maxAISBufferSize;
        GrowableByteBuffer byteBuffer = new GrowableByteBuffer(4096, 4096, maxSize);
        try {
            if(serializationType == SerializationType.NONE) {
                serializationType = DEFAULT_SERIALIZATION;
            }
            switch(serializationType) {
                case META_MODEL:
                    saveMetaModel(ex, byteBuffer, newAIS, getVolumeForSchemaTree(schemaName));
                break;
                case PROTOBUF:
                    saveProtobuf(ex, byteBuffer, newAIS, schemaName);
                break;
                default:
                    throw new IllegalStateException("Unsupported serialization: " + serializationType);
            }
        }
        catch(BufferOverflowException e) {
            throw new AISTooLargeException(maxSize);
        }
    }

    private void saveMetaModel(Exchange ex, GrowableByteBuffer buffer, AkibanInformationSchema newAIS, final String volume)
            throws PersistitException {
        buffer.clear();
        new TableSubsetWriter(new MessageTarget(buffer)) {
            @Override
            public boolean shouldSaveTable(Table table) {
                final String schemaName = table.getName().getSchemaName();
                return !schemaName.equals(TableName.AKIBAN_INFORMATION_SCHEMA) &&
                        getVolumeForSchemaTree(schemaName).equals(volume);
            }
        }.save(newAIS);
        buffer.flip();

        ex.clear().append(METAMODEL_PARENT_KEY);
        ex.getValue().clear();
        ex.getValue().putByteArray(buffer.array(), buffer.position(), buffer.limit());
        ex.store();
    }

    private void saveProtobuf(Exchange ex, GrowableByteBuffer buffer, AkibanInformationSchema newAIS, final String schema)
            throws PersistitException {
        final ProtobufWriter.TableSelector selector;
        if(TableName.AKIBAN_INFORMATION_SCHEMA.equals(schema)) {
            selector = new ProtobufWriter.SchemaSelector(schema) {
                @Override
                public boolean isSelected(UserTable table) {
                    return !legacyISTables.contains(table.getName()) &&
                           !table.hasMemoryTableFactory();
                }
            };
        } else {
            selector = new ProtobufWriter.SchemaSelector(schema);
        }

        ex.clear().append(PROTOBUF_PARENT_KEY).append(PROTOBUF_PSSM_VERSION).append(schema);
        if(newAIS.getSchema(schema) != null) {
            buffer.clear();
            new ProtobufWriter(buffer, selector).save(newAIS);
            buffer.flip();

            ex.getValue().clear().putByteArray(buffer.array(), buffer.position(), buffer.limit());
            ex.store();
        } else {
            ex.remove();
        }
    }

    private String getVolumeForSchemaTree(final String schemaName) {
        return treeService.volumeForTree(schemaName, SCHEMA_TREE_NAME);
    }

    /**
     * Internal helper intended to be called to finalize any AIS change. This includes create, delete,
     * alter, etc. This currently updates the {@link TreeService#SCHEMA_TREE_NAME} for a given schema,
     * rebuilds the {@link Store#getRowDefCache()}, and sets the {@link #getAis()} variable.
     *
     * @param session Session to run under
     * @param newAIS The new AIS to store on disk <b>and</b> commit as {@link #getAis()}.
     * @param schemaNames The schemas affected by the change.
     */
    private void commitAISChange(Session session, AkibanInformationSchema newAIS, Collection<String> schemaNames, boolean withRowDefCache) {
        newAIS.validate(AISValidations.LIVE_AIS_VALIDATIONS).throwIfNecessary();

        Exchange schemaEx = null;
        try {
            for(String schema : schemaNames) {
                TreeLink schemaTreeLink =  treeService.treeLink(schema, SCHEMA_TREE_NAME);
                schemaEx = treeService.getExchange(session, schemaTreeLink);
                saveAISToStorage(schemaEx, newAIS, schema);
                treeService.releaseExchange(session, schemaEx);
            }

            if(withRowDefCache) {
                try {
                    buildRowDefCache(newAIS);
                } catch (PersistitException e) {
                    LOG.error("AIS change successful and stored on disk but RowDefCache creation failed!");
                    LOG.error("RUNNING STATE NOW INCONSISTENT");
                    throw e;
                }
            }
        } catch(PersistitException e) {
            throw new PersistitAdapterException(e);
        } finally {
            if(schemaEx != null) {
                treeService.releaseExchange(session, schemaEx);
            }
        }
    }

    public static SerializationType detectSerializationType(Exchange ex) {
        try {
            SerializationType type = SerializationType.NONE;

            // Simple heuristic to determine which style AIS storage we have
            boolean hasMetaModel = ex.clear().append(METAMODEL_PARENT_KEY).isValueDefined();
            boolean hasProtobuf = ex.clear().append(PROTOBUF_PARENT_KEY).hasChildren();

            if(hasMetaModel && hasProtobuf) {
                throw new IllegalStateException("Both AIS and Protobuf serializations");
            }
            else if(hasMetaModel) {
                type = SerializationType.META_MODEL;
            }
            else if(hasProtobuf) {
                type = SerializationType.PROTOBUF;
            }
            else {
                ex.clear().append(Key.BEFORE);
                if(ex.next(true)) {
                    type = SerializationType.UNKNOWN;
                }
            }

            return type;
        } catch(PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    private void checkAndSetSerialization(SerializationType newSerializationType) {
        if((serializationType != SerializationType.NONE) && (serializationType != newSerializationType)) {
            throw new IllegalStateException("Mixed serialization types: " + serializationType + " vs " + newSerializationType);
        }
        serializationType = newSerializationType;
    }

    private void clearAndSaveAllAIS(Session session, AkibanInformationSchema newAIS) throws PersistitException {
        // Remove all existing trees
        treeService.visitStorage(session, new TreeVisitor() {
            @Override
            public void visit(Exchange ex) throws PersistitException {
                // Note: removeAll(), and not removeTree(), so we can fail and rollback safely
                ex.removeAll();
            }
        }, SCHEMA_TREE_NAME);

        // Re-serialize everything
        commitAISChange(session, newAIS, newAIS.getSchemas().keySet(), false);
    }

    private void performUpgrade(AkibanInformationSchema newAIS) throws PersistitException {
        switch(serializationType) {
            case PROTOBUF:      // Nothing to upgrade
                return;

            case NONE:          // No current data
                createUpgradedPrimordialTables(newAIS);
                return;

            case META_MODEL:    // Old data needing upgrade
                createUpgradedPrimordialTables(newAIS);
            break;

            default:
                throw new IllegalStateException("Unknown serialization: " + serializationType);
        }

        final Session session = sessionService.createSession();
        final Transaction transaction = treeService.getTransaction(session);
        transaction.begin();
        try {
            serializationType = SerializationType.PROTOBUF;
            clearAndSaveAllAIS(session, newAIS);

            Set<SerializationType> allTypes = getAllSerializationTypes(session);
            if(allTypes.size() != 1 || !allTypes.contains(SerializationType.PROTOBUF)) {
                throw new IllegalStateException("Upgrade left invalid serialization: " + allTypes);
            }

            transaction.commit();
        } finally {
            transaction.end();
            session.close();
        }
    }

    /**
     * @return All serialization types from all volumes
     */
    public Set<SerializationType> getAllSerializationTypes(Session session) {
        final Set<SerializationType> allTypes = EnumSet.noneOf(SerializationType.class);
        try {
            treeService.visitStorage(session, new TreeVisitor() {
                @Override
                public void visit(Exchange exchange) throws PersistitException {
                    allTypes.add(detectSerializationType(exchange));
                }
            }, SCHEMA_TREE_NAME);
        } catch(PersistitException e) {
            throw new PersistitAdapterException(e);
        }
        return allTypes;
    }

    /**
     * @return Current serialization type
     */
    public SerializationType getSerializationType() {
        return serializationType;
    }

    /**
     * @param serializationType Serialization type to use
     */
    public void setSerializationType(SerializationType serializationType) {
        this.serializationType = serializationType;
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

    private TableName createTableCommon(Session session, UserTable newTable, boolean isInternal,
                                        Integer version, MemoryTableFactory factory) {
        final TableName newName = newTable.getName();
        checkTableName(newName, false, isInternal);
        AISMerge merge = new AISMerge(getAis(), newTable);
        preserveExtraInfo(merge.getAIS(), getAis());
        merge.merge();
        if(factory != null) {
            merge.getAIS().getUserTable(newName).setMemoryTableFactory(factory);
        }
        if(version != null) {
            merge.getAIS().getUserTable(newName).setVersion(version);
        }
        commitAISChange(session, merge.getAIS(), Collections.singleton(newName.getSchemaName()), true);
        return getAis().getUserTable(newName).getName();
    }

    private static void checkAISSchema(TableName tableName, boolean shouldBeAIS) {
        final boolean isAIS = TableName.AKIBAN_INFORMATION_SCHEMA.equals(tableName.getSchemaName());
        if(shouldBeAIS && !isAIS) {
            throw new IllegalArgumentException("Table required to be in "+TableName.AKIBAN_INFORMATION_SCHEMA+" schema");
        }
        if(!shouldBeAIS && isAIS) {
            throw new ProtectedTableDDLException(tableName);
        }
    }

    private void checkExists(TableName tableName, boolean shouldExist) {
        final boolean tableExists = getAis().getTable(tableName) != null;
        if(shouldExist && !tableExists) {
            throw new NoSuchTableException(tableName);
        }
        if(!shouldExist && tableExists) {
            throw new DuplicateTableNameException(tableName);
        }
    }

    private void checkTableName(TableName tableName, boolean shouldExist, boolean aisAllowed) {
        checkAISSchema(tableName, aisAllowed);
        checkExists(tableName, shouldExist);
    }

    private static AkibanInformationSchema copyAIS(AkibanInformationSchema curAIS) {
        AkibanInformationSchema newAIS = new AkibanInformationSchema();
        return copyAIS(newAIS, curAIS, new Writer(new AISTarget(newAIS)));
    }

    private static AkibanInformationSchema copyAIS(AkibanInformationSchema newAIS, AkibanInformationSchema curAIS, Writer writer) {
        writer.save(curAIS);
        preserveExtraInfo(newAIS, curAIS);
        return newAIS;
    }

    private static void preserveExtraInfo(AkibanInformationSchema newAIS, AkibanInformationSchema curAIS) {
        for(UserTable table : curAIS.getSchema(TableName.AKIBAN_INFORMATION_SCHEMA).getUserTables().values()) {
            UserTable newTable = newAIS.getUserTable(table.getName());
            if(newTable != null) {
                if(table.hasMemoryTableFactory()) {
                    newTable.setMemoryTableFactory(table.getMemoryTableFactory());
                }
                if(table.hasVersion()) {
                    newTable.setVersion(table.getVersion());
                }
            }
        }
    }
}
