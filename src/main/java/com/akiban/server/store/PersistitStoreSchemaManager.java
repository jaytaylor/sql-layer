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

import static com.akiban.ais.ddl.SchemaDef.CREATE_TABLE;
import static com.akiban.server.service.tree.TreeService.SCHEMA_TREE_NAME;
import static com.akiban.server.service.tree.TreeService.STATUS_TREE_NAME;
import static com.akiban.server.store.PersistitStore.MAX_TRANSACTION_RETRY_COUNT;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
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
import com.akiban.ais.model.HKey;
import com.akiban.ais.model.HKeyColumn;
import com.akiban.ais.model.HKeySegment;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.Type;
import com.akiban.server.encoding.EncoderFactory;
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
import com.persistit.exception.PersistitException;
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

    private AkibanInformationSchema ais;

    private ServiceManager serviceManager;

    private RowDefCache rowDefCache;

    private AtomicLong updateTimestamp = new AtomicLong();

    private ByteBuffer byteBuffer = ByteBuffer.allocate(1<<20);

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

    private interface AISChangeCallback {
        public void beforeCommit(Exchange schemaExchange, TreeService treeService) throws Exception;
    }

    
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

    @Override
    public void alterTableAddIndexes(Session session, TableName tableName, Collection<Index> indexes) throws Exception {
        if(indexes.isEmpty()) {
            return;
        }

        final AkibanInformationSchema ais = getAis(session);
        final Table existingTable = ais.getTable(tableName);
        final Table firstIndexTable = indexes.iterator().next().getTable();

        if(existingTable == null) {
            throw new InvalidOperationException(ErrorCode.NO_SUCH_TABLE, "Unknown table: " + tableName);
        }
        if(!firstIndexTable.getTableId().equals(existingTable.getTableId())) {
            throw new InvalidOperationException(ErrorCode.NO_SUCH_TABLE, "TableId mismatch");
        }

        // Input validation: same table, not a primary key, not a duplicate index name, and
        // referenced columns are valid
        for(Index idx : indexes) {
            if(!idx.getTable().equals(firstIndexTable)) {
                throw new InvalidOperationException(ErrorCode.UNSUPPORTED_OPERATION,
                                                    "Cannot add indexes to multiple tables");
            }
            if(idx.isPrimaryKey()) {
                throw new InvalidOperationException(ErrorCode.UNSUPPORTED_OPERATION, "Cannot add primary key");
            }
            final String indexName = idx.getIndexName().getName();
            if(existingTable.getIndex(indexName) != null) {
                throw new InvalidOperationException(ErrorCode.DUPLICATE_KEY, "Index already exists: " + indexName);
            }
            for(IndexColumn idxCol : idx.getColumns()) {
                final Column refCol = idxCol.getColumn();
                final Column tableCol = existingTable.getColumn(refCol.getPosition());
                if (!refCol.getName().equals(tableCol.getName()) ||
                    !refCol.getType().equals(tableCol.getType())) {
                    throw new InvalidOperationException(ErrorCode.UNSUPPORTED_OPERATION,
                                                        "Index column does not match table column");
                }
            }
        }

        final AkibanInformationSchema newAIS = new AkibanInformationSchema();
        new Writer(new AISTarget(newAIS)).save(ais);
        final Table newTable = newAIS.getTable(tableName);

        // All OK, add to current table
        Integer curId = SchemaDefToAis.findMaxIndexIDInGroup(newAIS, newTable.getGroup());
        assert curId != null : newTable;
        for(Index idx : indexes) {
            Index newIndex = Index.create(newAIS, newTable, idx.getIndexName().getName(), ++curId,
                                          idx.isUnique(), idx.getConstraint());

            for(IndexColumn idxCol : idx.getColumns()) {
                Column refCol = newTable.getColumn(idxCol.getColumn().getPosition());
                IndexColumn indexCol = new IndexColumn(newIndex, refCol, idxCol.getPosition(),
                                                       idxCol.isAscending(), idxCol.getIndexedLength());
                newIndex.addColumn(indexCol);
            }
            newIndex.freezeColumns();
        }

        new AISBuilder(newAIS).generateGroupTableIndexes(newTable.getGroup());
        commitAISChange(session, newAIS, tableName.getSchemaName(), null);
    }

    @Override
    public void alterTableDropIndexes(Session session, TableName tableName, Collection<String> indexNames)
            throws Exception {
        if(indexNames.isEmpty()) {
            return;
        }

        final AkibanInformationSchema newAIS = new AkibanInformationSchema();
        new Writer(new AISTarget(newAIS)).save(ais);
        final Table newTable = newAIS.getTable(tableName);
        List<Index> indexesToDrop = new ArrayList<Index>();
        for(String indexName : indexNames) {
            final Index index = newTable.getIndex(indexName);
            indexesToDrop.add(index);
        }

        newTable.removeIndexes(indexesToDrop);
        new AISBuilder(newAIS).generateGroupTableIndexes(newTable.getGroup());
        commitAISChange(session, newAIS, tableName.getSchemaName(), null);
    }

    /**
     * Delete all table definition versions for a table specified by schema name
     * and table name. This removes the entire history of the table, and is
     * intended to implement part of the DROP TABLE operation (the other part is
     * truncating the data).
     * 
     * For a GroupTable, it will also delete all tables participating in the
     * group. A UserTable is required to have no referencing tables to succeed.
     * 
     * @param session
     * @param schemaName
     * @param tableName
     * @throws InvalidOperationException
     *             if removing this table would cause other table definitions to
     *             become invalid.
     */
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
    
    /**
     * Get the most recent version of a table definition identified by schema
     * and table names. If there is no such table this method returns
     * <code>null</code>.
     * 
     * @param session
     * @param schemaName
     * @param tableName
     */
    @Override
    public TableDefinition getTableDefinition(Session session, String schemaName, String tableName) throws Exception {
        final Table table = getAis(session).getTable(new TableName(schemaName, tableName));
        if(table == null) {
            return null;
        }
        final String ddl = new DDLGenerator().createTable(table);
        return new TableDefinition(table.getTableId(), schemaName, tableName, ddl);
    }

    /**
     * Get a map sorted by table name of all table definitions having the
     * specified schema name. The most recent definition for each name is
     * returned.
     * 
     * @param session
     * @param schemaName
     */
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

    /**
     * Get the Akiban Information Schema created from the the current set of
     * table definitions. This structure contains an internal representation of
     * most recent version of each table defined in the schema database, plus a
     * representation of the akiban_information_schema itself.
     * 
     * It would be more efficient to generate this value lazily, after multiple
     * table definitions have been created. However, the validateTableDefinition
     * method requires an up-to-date AIS, so for now we have to construct a new
     * AIS after every schema change.
     * 
     * @param session
     */
    @Override
    public AkibanInformationSchema getAis(final Session session) {
        return ais;
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
        // Old behavior, reassign group table IDs
        for(GroupTable groupTable: newAis.getGroupTables().values()) {
            final UserTable root = groupTable.getRoot();
            assert root != null : "Group with no root table: " + groupTable;
            groupTable.setTableId(TreeService.MAX_TABLES_PER_VOLUME - root.getTableId());
        }
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
                return table.isUserTable() && !tableNames.contains(table.getName());
            }
        }.save(ais);

        // Rebuild all group tables
        Queue<Join> tablesToAdd = new LinkedList<Join>();
        AISBuilder builder = new AISBuilder(newAis);
        for(UserTable table : newAis.getUserTables().values()) {
            if(table.getParentJoin() == null) {
                final Group group = table.getGroup();
                assert group != null : table;
                // recreate group table
                String groupTableName = "_akiban_" + group.getName();
                GroupTable groupTable = GroupTable.create(newAis, table.getName().getSchemaName(), groupTableName,
                                                          TreeService.MAX_TABLES_PER_VOLUME - table.getTableId());
                groupTable.setGroup(group);
                builder.addTableToGroup(group.getName(), table.getName().getSchemaName(), table.getName().getTableName());
                for(Join join : table.getCandidateChildJoins()) {
                    tablesToAdd.add(join);
                }
            }
            else {
                table.setGroup(null);
            }
        }
        while(!tablesToAdd.isEmpty()) {
            Join j = tablesToAdd.poll();
            builder.addJoinToGroup(j.getGroup().getName(), j.getName(), 0);
            for (Join join : j.getChild().getCandidateChildJoins()) {
                tablesToAdd.add(join);
            }
        }
        builder.groupingIsComplete();
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

    /**
     * Timestamp at or after the last schema change. The timestamp is a
     * universal, monotonically increasing counter maintained by Persistit.
     * Usually the value returned by this method is the timestamp associated
     * with the last schema change. However, when the Akiban Server starts up,
     * the timestamp is a value guaranteed to be greater than that of any schema
     * change. Clients can use the timestamp to determine whether locally cached
     * schema information is stale.
     * 
     * @return timestamp at or after last update
     */
    @Override
    public long getUpdateTimestamp() {
        return updateTimestamp.get();
    }

    @Override
    public int getSchemaGeneration() {
        final long ts = getUpdateTimestamp();
        return (int) ts ^ (int) (ts >>> 32);
    }

    /**
     * Updates the current timestamp to a new value greater than any previously
     * returned value. This method can be used to force clients that rely on a
     * timestamp value to determine staleness to refresh their cached schema
     * data.
     */
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
    }

    @Override
    public void stop() throws Exception {
        this.ais = null;
        this.rowDefCache = null;
        this.serviceManager = null;
    }

    @Override
    public void crash() throws Exception {
        stop();
    }

    /**
     * Create an AIS during the startup process so that the AIS tables are
     * populated and a visible. This can't be done until both this Service and
     * the PersistitStore service are fully initialized and registered, so it is
     * done as an "afterStart" step. It is done within the scope of a
     * transaction so that the TableStatus ordinal fields can be updated
     * transactionally in {@link RowDefCache#fixUpOrdinals()}.
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

                    // Load stored AIS data from each schema tree
                    treeService.visitStorage(session, new TreeVisitor() {
                        @Override
                        public void visit(Exchange ex) throws Exception {
                            ex.clear().append(BY_AIS);
                            if(ex.isValueDefined()) {
                                ex.fetch();

                                byte[] serializedAIS = ex.getValue().getByteArray();
                                ByteBuffer buffer = ByteBuffer.wrap(serializedAIS);
                                AkibanInformationSchema tmpAis= new Reader(new MessageSource(buffer)).load();
                                new Writer(new AISTarget(newAIS)).save(tmpAis);
                            }
                        }
                    }, SCHEMA_TREE_NAME);

                    forceNewTimestamp();
                    commitAIS(newAIS, updateTimestamp.get());
                    transaction.commit();
                    break;
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

    private void commitAIS(final AkibanInformationSchema newAis,
            final long timestamp) {
        final RowDefCache rowDefCache = getRowDefCache();
        rowDefCache.clear();
        serviceManager.getTreeService().getTableStatusCache().detachAIS();
        rowDefCache.setAIS(newAis);
        rowDefCache.fixUpOrdinals();
        updateTimestamp.set(timestamp);
        this.ais = newAis;
    }

    private RowDefCache getRowDefCache() {
        if (rowDefCache == null) {
            rowDefCache = serviceManager.getStore().getRowDefCache();
        }
        return rowDefCache;
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
     * Internal helper intended to be called to finalize any AIS change. This includes create, delete,
     * alter, etc. This currently updates the {@link TreeService#SCHEMA_TREE_NAME} for a given schema,
     * rebuilds the {@link #rowDefCache}, and sets the {@link #ais} variable.
     * @param session Session to run under
     * @param newAIS The new AIS to store in the {@link #BY_AIS} key range <b>and</b> commit as {@link #ais}.
     * @param schemaName The schema the change affected
     * @param callback If non-null, beforeCommit while be called before transaction.commit().
     * @throws Exception
     */
    private void commitAISChange(final Session session, final AkibanInformationSchema newAIS, final String schemaName,
                                 AISChangeCallback callback) throws Exception {
        byteBuffer.clear();
        new TableSubsetWriter(new MessageTarget(byteBuffer)) {
            @Override
            public boolean shouldSaveTable(Table table) {
                return table.getName().getSchemaName().equals(schemaName);
            }
        }.save(newAIS);
        byteBuffer.flip();

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
                schemaEx.getValue().putByteArray(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
                schemaEx.store();

                transaction.commit(new DefaultCommitListener() {
                    @Override
                    public void committed() {
                        commitAIS(newAIS, transaction.getCommitTimestamp());
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
