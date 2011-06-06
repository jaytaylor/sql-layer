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

import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.atomic.AtomicLong;

import com.akiban.ais.io.AISTarget;
import com.akiban.ais.io.MessageSource;
import com.akiban.ais.io.MessageTarget;
import com.akiban.ais.io.Reader;
import com.akiban.ais.io.TableSubsetWriter;
import com.akiban.ais.io.Writer;
import com.akiban.ais.model.AISBuilder;
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
import com.akiban.server.api.common.NoSuchGroupException;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.encoding.EncoderFactory;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.tree.TreeLink;
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
import com.akiban.message.ErrorCode;
import com.akiban.server.AkServer;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowDef;
import com.akiban.server.RowDefCache;
import com.akiban.server.service.AfterStart;
import com.akiban.server.service.Service;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.service.tree.TreeVisitor;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.Transaction;
import com.persistit.Transaction.DefaultCommitListener;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TransactionFailedException;

public class PersistitStoreSchemaManager implements Service<SchemaManager>,
        SchemaManager, AfterStart {

    final static int AIS_BASE_TABLE_ID = 1000000000;
    
    static final String AIS_DDL_NAME = "akiban_information_schema.ddl";

    static final String BY_NAME = "byName";

    static final String BY_AIS = "byAIS";

    private static final Logger LOG = LoggerFactory
            .getLogger(PersistitStoreSchemaManager.class.getName());

    private final static String CREATE_SCHEMA_FORMATTER = "create schema if not exists `%s`;";

    private final static String AKIBAN_INFORMATION_SCHEMA = "akiban_information_schema";

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

    private interface AISChangeCallback {
        public void beforeCommit(Exchange schemaExchange, TreeService treeService) throws Exception;
    }

    private AkibanInformationSchema ais;
    private ServiceManager serviceManager;
    private AtomicLong updateTimestamp;
    private int maxAISBufferSize;
    private ByteBuffer aisByteBuffer;
    
    @Override
    public TableName createTableDefinition(final Session session, final String defaultSchemaName,
                                           final String originalDDL) throws Exception {
        String ddlStatement = originalDDL;
        SchemaDef schemaDef = parseTableStatement(defaultSchemaName, ddlStatement);
        SchemaDef.UserTableDef tableDef = schemaDef.getCurrentTable();
        if (tableDef.isLikeTableDef()) {
            final SchemaDef.CName srcName = tableDef.getLikeCName();
            assert srcName.getSchema() != null : originalDDL;
            final Table srcTable = getAis(session).getTable(srcName.getSchema(), srcName.getName());
            if (srcTable == null) {
                throw new InvalidOperationException(ErrorCode.NO_SUCH_TABLE,
                        String.format("Unknown source table [%s] %s",
                                srcName.getSchema(), srcName.getName()));
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
        final Table curTable = getAis(session).getTable(schemaName, tableName);
        if (curTable != null) {
            throw new InvalidOperationException(ErrorCode.DUPLICATE_TABLE,
                    String.format("Table `%s`.`%s` already exists", schemaName, tableName));
        }

        validateTableDefinition(tableDef);
        final AkibanInformationSchema newAIS = addSchemaDefToAIS(schemaDef);
        final UserTable newTable = newAIS.getUserTable(schemaName, tableName);
        validateIndexSizes(newTable);

        commitAISChange(session, newAIS, schemaName, new AISChangeCallback() {
            @Override
            public void beforeCommit(Exchange schemaExchange, TreeService treeService) throws Exception {
                schemaExchange.clear().append(BY_NAME).append(schemaName).append(newTable.getName().getTableName());
                schemaExchange.append(newTable.getTableId().intValue());
                schemaExchange.getValue().put(originalDDL);
                schemaExchange.store();
            }
        });
        
        return newTable.getName();
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
    public void createIndexes(Session session, Collection<Index> indexesToAdd) throws Exception {
        final Map<String,String> volumeToSchema = new HashMap<String,String>();
        final AkibanInformationSchema newAIS = new AkibanInformationSchema();
        new Writer(new AISTarget(newAIS)).save(ais);
        final AISBuilder builder = new AISBuilder(newAIS);

        for(Index index : indexesToAdd) {
            if(index.isPrimaryKey()) {
                throw new InvalidOperationException(ErrorCode.UNSUPPORTED_OPERATION, "Cannot add primary key");
            }

            final Index curIndex;
            final Index newIndex;
            final Group newGroup;
            final String schemaName;
            final IndexName indexName = index.getIndexName();

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
                    Integer newId = SchemaDefToAis.findMaxIndexIDInGroup(newAIS, newGroup) + 1;
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
                    Integer newId = SchemaDefToAis.findMaxIndexIDInGroup(newAIS, newGroup) + 1;
                    newIndex = GroupIndex.create(newAIS, newGroup, indexName.getName(), newId, index.isUnique(),
                                                 index.getConstraint());
                }
                break;
                default:
                    throw new IllegalArgumentException("Unknown index type: " + index);
            }

            if(curIndex != null) {
                throw new InvalidOperationException(ErrorCode.UNSUPPORTED_OPERATION,
                                                    "Index already exists: " + indexName);
            }
            if(index.getColumns().isEmpty()) {
                throw new InvalidOperationException(ErrorCode.UNSUPPORTED_OPERATION, "Index has no columns: " + index);
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
                    throw new InvalidOperationException(ErrorCode.UNSUPPORTED_OPERATION,
                                                        "Table not in group: " + refTableName);
                }
                if(lastTable != null && !inSameBranch(lastTable, newRefTable)) {
                    throw new InvalidOperationException(ErrorCode.UNSUPPORTED_OPERATION,
                                                        "Branching group index: " + lastTable + "," + newRefTable);
                }
                lastTable = newRefTable;

                final Column column = indexCol.getColumn();
                final Column newColumn = newRefTable.getColumn(column.getName());
                if(newColumn == null) {
                    throw new InvalidOperationException(ErrorCode.UNSUPPORTED_OPERATION,
                                                        "Unknown column: " + column.getName());
                }
                if(!column.getType().equals(newColumn.getType())) {
                    throw new InvalidOperationException(ErrorCode.UNSUPPORTED_OPERATION,
                                                        "Column type mismatch: "  + column + "," + newColumn);
                }
                IndexColumn newIndexCol = new IndexColumn(newIndex, newColumn, indexCol.getPosition(),
                                                          indexCol.isAscending(), indexCol.getIndexedLength());
                newIndex.addColumn(newIndexCol);
            }

            newIndex.freezeColumns();
            builder.generateGroupTableIndexes(newGroup);
        }

        for(String schema : volumeToSchema.values()) {
            commitAISChange(session, newAIS, schema, null);
        }
    }

    @Override
    public void dropIndexes(Session session, Collection<Index> indexesToDrop) throws Exception {
        final AkibanInformationSchema newAIS = new AkibanInformationSchema();
        new Writer(new AISTarget(newAIS)).save(ais);
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
            commitAISChange(session, newAIS, schema, null);
        }
    }


    @Override
    public void deleteTableDefinition(final Session session, final String schemaName,
                                      final String tableName) throws Exception {
        if (AKIBAN_INFORMATION_SCHEMA.equals(schemaName)) {
            return;
        }

        final Table table = getAis(session).getTable(schemaName, tableName);
        if (table == null) {
            return;
        }

        final List<TableName> tables = new ArrayList<TableName>();
        if (table.isGroupTable() == true) {
            final Group group = table.getGroup();
            tables.add(group.getGroupTable().getName());
            for (final Table t : getAis(session).getUserTables().values()) {
                if (t.getGroup().equals(group)) {
                    tables.add(t.getName());
                }
            }
        } else if (table.isUserTable() == true) {
            final UserTable userTable = (UserTable) table;
            if (userTable.getChildJoins().isEmpty() == false) {
                throw new InvalidOperationException(
                        ErrorCode.UNSUPPORTED_MODIFICATION, table.getName()
                                + " has referencing tables");
            }
            if (userTable.getParentJoin() == null) {
                // Last table in group, also delete group table
                tables.add(userTable.getGroup().getGroupTable().getName());
            }
            tables.add(table.getName());
        }

        final AkibanInformationSchema newAIS = removeTablesFromAIS(tables);

        commitAISChange(session, newAIS,  schemaName, new AISChangeCallback() {
            @Override
            public void beforeCommit(Exchange schemaExchange, TreeService treeService) throws Exception {
                final TreeLink statusTreeLink = treeService.treeLink(schemaName, STATUS_TREE_NAME);
                final Exchange statusEx = treeService.getExchange(session, statusTreeLink);
                try {
                    deleteTableDefinitions(tables, schemaExchange, statusEx);
                }
                finally {
                    treeService.releaseExchange(session, schemaExchange);
                }
            }
        });
    }

    private void deleteTableDefinitions(List<TableName> tables, Exchange schemaEx, Exchange statusEx) throws Exception {
        for(final TableName tn : tables) {
            schemaEx.clear().append(BY_NAME).append(tn.getSchemaName()).append(tn.getTableName());
            final KeyFilter keyFilter = new KeyFilter(schemaEx.getKey(), 4, 4);
            schemaEx.clear();
            while (schemaEx.next(keyFilter)) {
                final int tableId = schemaEx.getKey().indexTo(-1).decodeInt();
                schemaEx.remove();
                statusEx.clear().append(tableId).remove();
                serviceManager.getTreeService().getTableStatusCache().drop(tableId);
            }
        }
    }

    @Override
    public TableDefinition getTableDefinition(Session session, TableName tableName) throws NoSuchTableException {
        final Table table = getAis(session).getTable(tableName);
        if(table == null) {
            throw new NoSuchTableException(tableName);
        }
        final String ddl = new DDLGenerator().createTable(table);
        return new TableDefinition(table.getTableId(), tableName, ddl);
    }

    @Override
    public SortedMap<String, TableDefinition> getTableDefinitions(Session session, String schemaName) throws Exception {
        final SortedMap<String, TableDefinition> result = new TreeMap<String, TableDefinition>();
        final DDLGenerator gen = new DDLGenerator();
        for(UserTable table : getAis(session).getUserTables().values()) {
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
    public AkibanInformationSchema getAis(final Session session) {
        return ais;
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
    private AkibanInformationSchema addSchemaDefToAIS(SchemaDef schemaDef) throws Exception {
        AkibanInformationSchema newAis = new AkibanInformationSchema();
        new Writer(new AISTarget(newAis)).save(ais);
        new SchemaDefToAis(schemaDef, newAis, true).getAis();
        setGroupTableIds(newAis);
        return newAis;
    }

    /**
     * Construct a new AIS instance containg a copy of the currently known data, see @{link #ais},
     * minus the given list of TableNames.
     * @param tableNames List of tables to exclude from new AIS.
     * @return A completely new AIS.
     * @throws Exception For any error.
     */
    private AkibanInformationSchema removeTablesFromAIS(final List<TableName> tableNames) throws Exception {
        AkibanInformationSchema newAis = new AkibanInformationSchema();
        new TableSubsetWriter(new AISTarget(newAis)) {
            @Override
            public boolean shouldSaveTable(Table table) {
                return !tableNames.contains(table.getName());
            }
        }.save(ais);

        // Fix up group table columns and indexes for modified groups
        AISBuilder builder = new AISBuilder(newAis);
        Set<String> handledGroups = new HashSet<String>();
        for(TableName tn : tableNames) {
            final UserTable userTable = ais.getUserTable(tn);
            if(userTable != null) {
                final String groupName = userTable.getGroup().getName();
                final Group group = newAis.getGroup(groupName);
                if(group != null && !handledGroups.contains(groupName)) {
                    builder.generateGroupTableColumns(group);
                    builder.generateGroupTableIndexes(group);
                    handledGroups.add(groupName);
                }
            }
        }
        return newAis;
    }

    @Override
    public List<String> schemaStrings(Session session, boolean withGroupTables) throws Exception {
        final AkibanInformationSchema ais = getAis(session);
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
        final TreeService treeService = serviceManager.getTreeService();
        Session session = ServiceManagerImpl.newSession();
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
    public void start() throws Exception {
        serviceManager = ServiceManagerImpl.get();
        updateTimestamp = new AtomicLong();
        ConfigurationService config = serviceManager.getConfigurationService();
        maxAISBufferSize = Integer.parseInt(config.getProperty(MAX_AIS_SIZE_PROPERTY));
        if(maxAISBufferSize < 0) {
            LOG.warn("Clamping property "+MAX_AIS_SIZE_PROPERTY+" to 0");
            maxAISBufferSize = 0;
        }
        // 0 = unlimited, start off at 1MB in this case.
        aisByteBuffer = ByteBuffer.allocate(maxAISBufferSize != 0 ? maxAISBufferSize : 1<<20);
    }

    @Override
    public void stop() throws Exception {
        this.ais = null;
        this.serviceManager = null;
        this.updateTimestamp = null;
        this.maxAISBufferSize = 0;
        this.aisByteBuffer = null;
    }

    @Override
    public void crash() throws Exception {
        stop();
    }

    /**
     * Load the AIS tables from file and then load the serialized AIS from each
     * {@link TreeService#SCHEMA_TREE_NAME} in each volume. This must be done
     * in afterStart because it requires other services, TreeService and Store
     * specifically, to be up and functional
     */
    @Override
    public void afterStart() throws Exception {
        final TreeService treeService = serviceManager.getTreeService();
        final Session session = ServiceManagerImpl.newSession();
        final Transaction transaction = treeService.getTransaction(session);
        int retries = MAX_TRANSACTION_RETRY_COUNT;
        InputStream aisFileStream = null;
        try {
            for (;;) {
                try {
                    transaction.begin();

                    // Create AIS tables
                    aisFileStream = AkServer.class.getClassLoader().getResourceAsStream(AIS_DDL_NAME);
                    SchemaDef schemaDef = SchemaDef.parseSchemaFromStream(aisFileStream);
                    final AkibanInformationSchema newAIS = new SchemaDefToAis(schemaDef, true).getAis();
                    int curId = AIS_BASE_TABLE_ID;
                    for(UserTable table : newAIS.getUserTables().values()) {
                        table.setTableId(curId++);
                    }
                    setGroupTableIds(newAIS);

                    // Load stored AIS data from each schema tree
                    treeService.visitStorage(session, new TreeVisitor() {
                                                 @Override
                                                 public void visit(Exchange ex) throws Exception {
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
                        throw new TransactionFailedException();
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
            final RowDefCache rowDefCache = serviceManager.getStore().getRowDefCache();
            rowDefCache.clear();
            serviceManager.getTreeService().getTableStatusCache().detachAIS();
            rowDefCache.setAIS(newAis);
            updateTimestamp.set(timestamp);
            this.ais = newAis;
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
        try {
            SchemaDef def = new SchemaDef();
            def.setMasterSchemaName(defaultSchemaName);
            def.parseCreateTable(ddl);
            return def;
        } catch (Exception e1) {
            throw new InvalidOperationException(ErrorCode.PARSE_EXCEPTION,
                    String.format("[%s] %s: %s", defaultSchemaName,
                            e1.getMessage(), ddl), e1);
        }
    }

    private void complainAboutIndexDataType(String schema, String table,
            String index, String column, String type)
            throws InvalidOperationException {
        throw new InvalidOperationException(
                ErrorCode.UNSUPPORTED_INDEX_DATA_TYPE,
                "Table `%s`.`%s` index `%s` has unsupported type `%s` from column `%s`",
                schema, table, index, type, column);
    }

    private void validateTableDefinition(final SchemaDef.UserTableDef tableDef)
            throws Exception {
        final String schemaName = tableDef.getCName().getSchema();
        final String tableName = tableDef.getCName().getName();
        if (AKIBAN_INFORMATION_SCHEMA.equals(schemaName)) {
            throw new InvalidOperationException(ErrorCode.PROTECTED_TABLE,
                    "Cannot create table `%s` in protected schema `%s`",
                    tableName, schemaName);
        }

        final String tableCharset = tableDef.getCharset();
        if (tableCharset != null && !Charset.isSupported(tableCharset)) {
            throw new InvalidOperationException(ErrorCode.UNSUPPORTED_CHARSET,
                    "Table `%s`.`%s` has unsupported default charset %s",
                    schemaName, tableName, tableCharset);
        }

        for (SchemaDef.ColumnDef col : tableDef.getColumns()) {
            final String typeName = col.getType();
            if (!ais.isTypeSupported(typeName)) {
                throw new InvalidOperationException(
                        ErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Table `%s`.`%s` column `%s` is unsupported type %s",
                        schemaName, tableName, col.getName(), typeName);
            }
            final String charset = col.getCharset();
            if (charset != null && !Charset.isSupported(charset)) {
                throw new InvalidOperationException(
                        ErrorCode.UNSUPPORTED_CHARSET,
                        "Table `%s`.`%s` column `%s` has unsupported charset %s",
                        schemaName, tableName, col.getName(), charset);
            }
        }

        for (SchemaDef.IndexColumnDef colDef : tableDef.getPrimaryKey().getColumns()) {
            final SchemaDef.ColumnDef col = tableDef.getColumn(colDef.getColumnName());
            if (col != null) {
                final String typeName = col.getType();
                if (!ais.isTypeSupportedAsIndex(typeName)) {
                    complainAboutIndexDataType(schemaName, tableName,
                            "PRIMARY", colDef.getColumnName(), typeName);
                }
            }
        }

        for (SchemaDef.IndexDef index : tableDef.getIndexes()) {
            for (String colName : index.getColumnNames()) {
                final SchemaDef.ColumnDef col = tableDef.getColumn(colName);
                if (col != null) {
                    final String typeName = col.getType();
                    if (!ais.isTypeSupportedAsIndex(typeName)) {
                        complainAboutIndexDataType(schemaName, tableName,
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
            throw new InvalidOperationException(
                    ErrorCode.JOIN_TO_MULTIPLE_PARENTS,
                    "Table `%s`.`%s` joins to more than one table", schemaName,
                    tableName);
        }

        final SchemaDef.ReferenceDef parentJoin = parentJoins.get(0);
        final String parentTableName = parentJoin.getTableName();
        final String parentSchema = parentJoin.getSchemaName() != null ? parentJoin
                .getSchemaName() : schemaName;

        if (AKIBAN_INFORMATION_SCHEMA.equals(parentSchema)) {
            throw new InvalidOperationException(
                    ErrorCode.JOIN_TO_PROTECTED_TABLE,
                    "Table `%s`.`%s` joins to protected table `%s`.`%s`",
                    schemaName, tableName, parentSchema, parentTableName);
        }

        final UserTable parentTable = ais.getUserTable(parentSchema,
                parentTableName);
        if (schemaName.equals(parentSchema)
                && tableName.equals(parentTableName) || parentTable == null) {
            throw new InvalidOperationException(
                    ErrorCode.JOIN_TO_UNKNOWN_TABLE,
                    "Table `%s`.`%s` joins to undefined table `%s`.`%s`",
                    schemaName, tableName, parentSchema, parentTableName);
        }

        List<String> childColumns = parentJoin.getIndex().getColumnNames();
        List<String> parentColumns = parentJoin.getColumns();
        List<Column> parentPKColumns = parentTable.getPrimaryKey() == null ? null
                : parentTable.getPrimaryKey().getColumns();
        if (parentColumns.size() != childColumns.size()
                || parentPKColumns == null
                || parentColumns.size() != parentPKColumns.size()) {
            throw new InvalidOperationException(
                    ErrorCode.JOIN_TO_WRONG_COLUMNS,
                    "Table `%s`.`%s` join reference and `%s`.`%s` primary key parts must match",
                    schemaName, tableName, parentSchema, parentTableName);
        }

        Iterator<String> childColumnIt = childColumns.iterator();
        Iterator<Column> parentPKIt = parentPKColumns.iterator();
        for (String parentColumnName : parentColumns) {
            // Check same columns
            String childColumnName = childColumnIt.next();
            Column parentPKColumn = parentPKIt.next();
            if (!parentColumnName.equalsIgnoreCase(parentPKColumn.getName())) {
                throw new InvalidOperationException(
                        ErrorCode.JOIN_TO_WRONG_COLUMNS,
                        "Table `%s`.`%s` join reference part `%s` does not match `%s`.`%s` primary key part `%s`",
                        schemaName, tableName, parentColumnName, parentSchema,
                        parentTableName, parentPKColumn.getName());
            }
            // Check child column exists
            SchemaDef.ColumnDef columnDef = tableDef.getColumn(childColumnName);
            if (columnDef == null) {
                throw new InvalidOperationException(
                        ErrorCode.JOIN_TO_WRONG_COLUMNS,
                        "Table `%s`.`%s` join reference contains unknown column `%s`",
                        schemaName, tableName, childColumnName);
            }
            // Check child and parent column types
            final String type = columnDef.getType();
            final String parentType = parentPKColumn.getType().name();
            if (!ais.canTypesBeJoined(parentType, type)) {
                throw new InvalidOperationException(
                        ErrorCode.JOIN_TO_WRONG_COLUMNS,
                        "Table `%s`.`%s` column `%s` [%s] cannot be joined to `%s`.`%s` column `%s` [%s]",
                        schemaName, tableName, columnDef.getName(), type,
                        parentSchema, parentTableName,
                        parentPKColumn.getName(), parentType);
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
            throw new InvalidOperationException(ErrorCode.UNSUPPORTED_INDEX_SIZE,
                String.format("Table `%s`.`%s` HKEY exceeds maximum key size",
                              table.getName().getSchemaName(), table.getName().getTableName()));
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
                    throw new InvalidOperationException(ErrorCode.UNSUPPORTED_INDEX_SIZE,
                        String.format("Table `%s`.`%s` unique index `%s` has prefix size",
                                       table.getName().getSchemaName(), table.getName().getTableName(),
                                       index.getIndexName().getName()));

                }
            }
            if(fullKeySize > MAX_INDEX_STORAGE_SIZE) {
                throw new InvalidOperationException(ErrorCode.UNSUPPORTED_INDEX_SIZE,
                    String.format("Table `%s`.`%s` index `%s` exceeds maximum key size",
                                  table.getName().getSchemaName(), table.getName().getTableName(),
                                  index.getIndexName().getName()));
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
    private ByteBuffer trySerializeAIS(final AkibanInformationSchema newAIS, final String volumeName) throws Exception {
        boolean finishedSerializing = false;
        while(!finishedSerializing) {
            try {
                aisByteBuffer.clear();
                new TableSubsetWriter(new MessageTarget(aisByteBuffer)) {
                    @Override
                    public boolean shouldSaveTable(Table table) {
                        final String schemaName = table.getName().getSchemaName();
                        return !schemaName.equals(AKIBAN_INFORMATION_SCHEMA) &&
                               getVolumeForSchemaTree(schemaName).equals(volumeName);
                    }
                }.save(newAIS);
                aisByteBuffer.flip();
                finishedSerializing = true;
            }
            catch(BufferOverflowException e) {
                if(aisByteBuffer.capacity() == maxAISBufferSize) {
                    throw new InvalidOperationException(ErrorCode.INTERNAL_ERROR,
                                                        "Serialized AIS exceeds max size ("+ maxAISBufferSize +")", e);
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
        return serviceManager.getTreeService().volumeForTree(schemaName, SCHEMA_TREE_NAME);
    }

    /**
     * Internal helper intended to be called to finalize any AIS change. This includes create, delete,
     * alter, etc. This currently updates the {@link TreeService#SCHEMA_TREE_NAME} for a given schema,
     * rebuilds the {@link Store#getRowDefCache()}, and sets the {@link #ais} variable.
     * @param session Session to run under
     * @param newAIS The new AIS to store in the {@link #BY_AIS} key range <b>and</b> commit as {@link #ais}.
     * @param schemaName The schema the change affected.
     * @param callback If non-null, beforeCommit while be called before transaction.commit().
     * @throws Exception for any error.
     */
    private void commitAISChange(final Session session, final AkibanInformationSchema newAIS, final String schemaName,
                                 AISChangeCallback callback) throws Exception {
        ByteBuffer buffer = trySerializeAIS(newAIS, getVolumeForSchemaTree(schemaName));
        final TreeService treeService = serviceManager.getTreeService();
        final Transaction transaction = treeService.getTransaction(session);
        int retries = MAX_TRANSACTION_RETRY_COUNT;
        for(;;) {
            final TreeLink schemaTreeLink =  treeService.treeLink(schemaName, SCHEMA_TREE_NAME);
            final Exchange schemaEx = treeService.getExchange(session, schemaTreeLink);
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
                treeService.releaseExchange(session, schemaEx);
            }
        }
    }
}
