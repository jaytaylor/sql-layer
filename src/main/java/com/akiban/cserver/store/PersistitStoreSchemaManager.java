package com.akiban.cserver.store;

import static com.akiban.ais.ddl.SchemaDef.CREATE_TABLE;
import static com.akiban.cserver.service.tree.TreeService.AIS_BASE_TABLE_ID;
import static com.akiban.cserver.service.tree.TreeService.SCHEMA_TREE_NAME;
import static com.akiban.cserver.service.tree.TreeService.STATUS_TREE_NAME;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.ais.ddl.SchemaDef;
import com.akiban.ais.ddl.SchemaDefToAis;
import com.akiban.ais.io.Writer;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.cserver.CServer;
import com.akiban.cserver.CServerAisTarget;
import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.TableStatus;
import com.akiban.cserver.service.AfterStart;
import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.ServiceManager;
import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.cserver.service.tree.TreeCache;
import com.akiban.cserver.service.tree.TreeLink;
import com.akiban.cserver.service.tree.TreeService;
import com.akiban.cserver.service.tree.TreeVisitor;
import com.akiban.message.ErrorCode;
import com.akiban.util.MySqlStatementSplitter;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.exception.PersistitException;

public class PersistitStoreSchemaManager implements Service<SchemaManager>,
        SchemaManager, AfterStart {

    static final String AIS_DDL_NAME = "akiba_information_schema.ddl";

    static final String BY_ID = "byId";

    static final String BY_NAME = "byName";

    private static final Log LOG = LogFactory
            .getLog(PersistitStoreSchemaManager.class.getName());

    // TODO - replace with transactional cache implementation
    private final static long DELAY = 10000L;

    private final static String CREATE_SCHEMA_IF_NOT_EXISTS = "create schema if not exists ";

    private final static String SEMI_COLON = ";";

    private final static String AKIBAN_INFORMATION_SCHEMA = "akiba_information_schema";

    private static List<TableDefinition> aisSchema = readAisSchema();

    private SchemaDef schemaDef;

    private AkibaInformationSchema ais;

    private long aisTimestamp;

    private ServiceManager serviceManager;

    private RowDefCache rowDefCache;

    private long saveTimestamp;

    private final Map<String, TreeLink> schemaLinkMap = new HashMap<String, TreeLink>();

    private final Map<String, TreeLink> statusLinkMap = new HashMap<String, TreeLink>();

    private final Timer timer = new Timer("TableStatus_Flusher", true);

    /**
     * Create or update a table definition given a schema name, table name and a
     * CREATE TABLE statement supplied by the client. The CREATE TABLE
     * statement, if correctly formed, supplies the table name. It optionally
     * specifies a schema name, and if so, that name overrides the supplied
     * default schema name.
     * 
     * This method verifies the integrity of the supplied statement as follows:
     * <ul>
     * <li>it must be valid DDL syntax</li>
     * <li>it does not have a schema name reserved by Akiban</li>
     * <li>any references to other tables and columns must be valid</li>
     * <li>TODO: any previously existing table definition having the same name
     * is compatible, meaning that rows already stored under a previously
     * existing definition of a table having the same schema name and table name
     * can be transformed to a row of the new format.</li>
     * </ul>
     * 
     * @param session
     * @param defaultSchemaName
     *            default schema name (as supplied by a USE statement in the
     *            client) to be used in case the statement does not explicitly
     *            specify a schema name.
     * @param statement
     *            a CREATE TABLE statement
     * @throws InvalidOperationException
     *             if the statement does not match these criteria
     * @throws PersistitException
     *             if any of the underlying B-Tree operations caused an
     *             exception
     */
    @Override
    public void createTableDefinition(final Session session,
            final String defaultSchemaName, final String statement, final boolean useOldId)
            throws Exception {
        final TreeService treeService = serviceManager.getTreeService();
        String canonical = SchemaDef.canonicalStatement(statement);
        final SchemaDef.UserTableDef tableDef = parseTableStatement(
                defaultSchemaName, canonical);
        String schemaName = tableDef.getCName().getSchema();
        if (schemaName == null) {
            final StringBuilder sb = new StringBuilder(CREATE_TABLE);
            schemaName = defaultSchemaName;
            TableName.escape(schemaName, sb);
            sb.append(".");
            sb.append(naked(canonical));
            canonical = sb.toString();
        }
        final String tableName = tableDef.getCName().getName();

        validateTableDefinition(session, schemaName, statement, tableDef);
        final Exchange ex = treeService.getExchange(session,
                treeLink(schemaName, SCHEMA_TREE_NAME));

        try {
            if (ex.clear().append(BY_NAME).append(schemaName).append(tableName)
                    .append(Key.AFTER).previous()) {
                final int tableId = ex.getKey().indexTo(-1).decodeInt();
                ex.clear().append(BY_ID).append(tableId).fetch();
                final String previousValue = ex.getValue().getString();
                if (canonical.equals(previousValue)) {
                    return;
                }
                else if(useOldId == true) {
                    ex.getValue().put(canonical);
                    ex.clear().append(BY_ID).append(tableId).store();
                }
            }
            else {
                final int tableId;
                if (ex.clear().append(BY_ID).append(Key.AFTER).previous()) {
                    tableId = ex.getKey().indexTo(1).decodeInt() + 1;
                } else {
                    tableId = 1;
                }
                ex.getValue().put(canonical);
                ex.clear().append(BY_ID).append(tableId).store();
                ex.getValue().putNull();
                ex.clear().append(BY_NAME).append(schemaName).append(tableName)
                        .append(tableId).store();
            }

            changed(treeService, session);
            saveTableStatusRecords(session);
            return;
        } finally {
            treeService.releaseExchange(session, ex);
        }
    }

    /**
     * Delete a table definition version specified by tableId. There is no
     * validity checking for this call; the definition is simply removed. DROP
     * TABLE should use the
     * {@link #deleteTableDefinition(Session, String, String)} method to ensure
     * integrity.
     * 
     * @param session
     * @param tableId
     *            tableId of a table definition version to be deleted
     */
    @Override
    public void deleteTableDefinition(Session session, final int tableId)
            throws Exception {
        final TreeService treeService = serviceManager.getTreeService();
        treeService.visitStorage(session, new TreeVisitor() {

            @Override
            public void visit(Exchange exchange) throws Exception {
                exchange.clear().append(BY_ID).append(tableId).fetch();
                if (exchange.isValueDefined()) {
                    final String canonical = exchange.getValue().getString();
                    SchemaDef.CName cname = cname(canonical);
                    exchange.remove();
                    exchange.clear().append(BY_NAME).append(cname.getSchema())
                            .append(cname.getName()).append(tableId).remove();
                }
            }

        }, SCHEMA_TREE_NAME);
        removeStaleTableStatusRecords(session);
        saveTableStatusRecords(session);
    }

    /**
     * Delete all table definition versions for a table specified by schema name
     * and table name. This removes the entire history of the table, and is
     * intended to implement part of the DROP TABLE operation. (The other part
     * is truncating the data.)
     * 
     * TODO: This method verifies that no other tables refer to this table
     * definition before removing it. An attempt to remove a table definition
     * that would render other tables invalid is rejected.
     * 
     * @param session
     * @param schemaName
     * @param tableName
     * @throws InvalidOperationException
     *             if removing this table would cause other table definitions to
     *             become invalid.
     */
    @Override
    public void deleteTableDefinition(final Session session,
            final String schemaName, final String tableName) throws Exception {
        if (AKIBAN_INFORMATION_SCHEMA.equals(schemaName)) {
            return;
        }
        // TODO - This is temporary. this method finds all the members of the
        // group containing specified table, and deletes them all. Note - this
        // implementation requires an up-to-date AIS, which is created lazily as
        // a side-effect.
        //
        final AkibaInformationSchema ais = getAis(session);
        final Table table = ais.getTable(schemaName, tableName);
        if (table == null) {
            return;
        }

        final List<TableName> tables = new ArrayList<TableName>();
        final Group group = table.getGroup();
        tables.add(group.getGroupTable().getName());
        for (final Table t : ais.getUserTables().values()) {
            if (t.getGroup().equals(group)) {
                tables.add(t.getName());
            }
        }
        deleteTableDefinitionList(session, tables);
        removeStaleTableStatusRecords(session);
        saveTableStatusRecords(session);
    }

    private void deleteTableDefinitionList(final Session session,
            final List<TableName> tables) throws Exception {
        final TreeService treeService = serviceManager.getTreeService();

        for (final TableName tn : tables) {
            final String schemaName = tn.getSchemaName();
            final String tableName = tn.getTableName();
            Exchange ex1 = null;
            Exchange ex2 = null;
            Exchange ex3 = null;
            try {
                ex1 = treeService.getExchange(session,
                        treeLink(schemaName, SCHEMA_TREE_NAME));
                ex2 = treeService.getExchange(session,
                        treeLink(schemaName, SCHEMA_TREE_NAME));
                ex3 = treeService.getExchange(session,
                        treeLink(schemaName, STATUS_TREE_NAME));
                ex1.clear().append(BY_NAME).append(schemaName)
                        .append(tableName);
                final KeyFilter keyFilter = new KeyFilter(ex1.getKey(), 4, 4);

                ex1.clear();
                while (ex1.next(keyFilter)) {
                    final int tableId = ex1.getKey().indexTo(-1).decodeInt();
                    ex2.clear().append(BY_ID).append(tableId).remove();
                    ex1.remove();
                    ex3.clear().append(tableId).remove();
                }
                changed(treeService, session);
            } catch (PersistitException e) {
                LOG.error("Failed to delete table " + schemaName + "."
                        + tableName, e);
                throw e;
            } finally {
                if (ex1 != null) {
                    treeService.releaseExchange(session, ex1);
                }
                if (ex2 != null) {
                    treeService.releaseExchange(session, ex2);
                }
                if (ex3 != null) {
                    treeService.releaseExchange(session, ex3);
                }
            }
        }
    }

    /**
     * Delete all table definition versions for all tables contained in a named
     * schema. This removes the entire history of all tables in the schema, and
     * is intended to implement part of the DROP SCHEMA operation. (The other
     * part is truncating the data.)
     * 
     * This method verifies that no other tables refer to these table
     * definitions before removing them. An attempt to remove a table definition
     * that would render other tables invalid is rejected.
     * 
     * @param session
     * @param schemaName
     * @throws InvalidOperationException
     *             if removing a table in this schema would cause other table
     *             definitions to become invalid.
     */
    @Override
    public void deleteSchemaDefinition(final Session session,
            final String schemaName) throws Exception {
        if (AKIBAN_INFORMATION_SCHEMA.equals(schemaName)) {
            return;
        }
        final TreeService treeService = serviceManager.getTreeService();
        Exchange ex1 = null;
        Exchange ex2 = null;
        try {
            ex1 = treeService.getExchange(session,
                    treeLink(schemaName, SCHEMA_TREE_NAME));
            ex2 = treeService.getExchange(session,
                    treeLink(schemaName, SCHEMA_TREE_NAME));
            ex1.clear().append(BY_NAME).append(schemaName);
            final KeyFilter keyFilter = new KeyFilter(ex1.getKey(), 4, 4);

            ex1.clear();
            while (ex1.next(keyFilter)) {
                final int tableId = ex1.getKey().indexTo(-1).decodeInt();
                ex2.clear().append(BY_ID).append(tableId).remove();
                ex1.remove();
            }
            changed(treeService, session);
        } catch (PersistitException e) {
            LOG.error("Failed to delete schema " + schemaName, e);
            throw e;
        } finally {
            if (ex1 != null) {
                treeService.releaseExchange(session, ex1);
            }
            if (ex2 != null) {
                treeService.releaseExchange(session, ex2);
            }
        }

    }

    /**
     * Remove all table definitions. This visits all volumes and deletes the
     * schema trees from all of them. Use with care!
     * 
     * @param session
     */
    @Override
    public void deleteAllDefinitions(final Session session) throws Exception {
        final TreeService treeService = serviceManager.getTreeService();
        treeService.visitStorage(session, new TreeVisitor() {

            @Override
            public void visit(final Exchange exchange) throws Exception {
                exchange.clear().removeAll();
            }

        }, SCHEMA_TREE_NAME);
        changed(treeService, session);
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
    public TableDefinition getTableDefinition(Session session,
            String schemaName, String tableName) throws Exception {
        if (AKIBAN_INFORMATION_SCHEMA.equals(schemaName)) {
            for (final TableDefinition td : aisSchema) {
                if (td.getTableName().equals(tableName)) {
                    return td;
                }
            }
            return null;
        }
        final TreeService treeService = serviceManager.getTreeService();
        final Exchange ex = treeService.getExchange(session,
                treeLink(schemaName, SCHEMA_TREE_NAME));
        try {
            if (ex.clear().append(BY_NAME).append(schemaName).append(tableName)
                    .append(Key.AFTER).previous()) {
                final int tableId = ex.getKey().indexTo(-1).decodeInt();
                ex.clear().append(BY_ID).append(tableId).fetch();
                return new TableDefinition(tableId, schemaName, tableName, ex
                        .getValue().getString());
            } else {
                return null;
            }

        } finally {
            treeService.releaseExchange(session, ex);
        }
    }

    /**
     * Get a list containing all versions of table definitions for a specified
     * schema and table name. The list is arranged in forward-chronological
     * order, which is determined by increasing values of tableId. If there is
     * no such table, this method returns an empty list.
     * 
     * @param session
     * @param schemaName
     * @param tableName
     */
    @Override
    public List<TableDefinition> getTableDefinitionHistory(Session session,
            String schemaName, String tableName) throws Exception {
        final List<TableDefinition> result = new ArrayList<TableDefinition>();
        if (AKIBAN_INFORMATION_SCHEMA.equals(schemaName)) {
            for (final TableDefinition td : aisSchema) {
                if (td.getTableName().equals(tableName)) {
                    result.add(td);
                }
            }
            return result;
        }
        final TreeService treeService = serviceManager.getTreeService();
        final Exchange ex = treeService.getExchange(session,
                treeLink(schemaName, SCHEMA_TREE_NAME));
        ex.clear().append(BY_NAME).append(schemaName).append(tableName)
                .append(Key.BEFORE);
        try {
            while (ex.next()) {
                final int tableId = ex.getKey().indexTo(-1).decodeInt();
                ex.clear().append(BY_ID).append(tableId).fetch();
                result.add(new TableDefinition(tableId, schemaName, tableName,
                        ex.getValue().getString()));
            }
        } finally {
            treeService.releaseExchange(session, ex);
        }
        return result;
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
    public SortedMap<String, TableDefinition> getTableDefinitions(
            Session session, String schemaName) throws Exception {
        final SortedMap<String, TableDefinition> result = new TreeMap<String, TableDefinition>();
        if (AKIBAN_INFORMATION_SCHEMA.equals(schemaName)) {
            for (final TableDefinition td : aisSchema) {
                result.put(td.getTableName(), td);
            }
            return result;
        }
        final TreeService treeService = serviceManager.getTreeService();
        final Exchange ex1 = treeService.getExchange(session,
                treeLink(schemaName, SCHEMA_TREE_NAME));
        final Exchange ex2 = treeService.getExchange(session,
                treeLink(schemaName, SCHEMA_TREE_NAME));
        ex1.clear().append(BY_NAME).append(schemaName).append(Key.BEFORE);
        try {
            while (ex1.next()) {
                final String tableName = ex1.getKey().indexTo(-1)
                        .decodeString();
                if (ex1.append(Key.AFTER).previous()) {
                    final int tableId = ex1.getKey().indexTo(-1).decodeInt();
                    ex2.clear().append(BY_ID).append(tableId).fetch();
                    result.put(tableName, new TableDefinition(tableId,
                            schemaName, tableName, ex2.getValue().getString()));
                }
                ex1.cut();
            }
            return result;
        } finally {
            treeService.releaseExchange(session, ex1);
        }
    }

    /**
     * Get the Akiban Information Schema created from the the current set of
     * table definitions. This structure contains an internal representation of
     * most recent version of each table defined in the schema database, plus a
     * representation of the akiban_information_schema itself.
     * 
     * This method returns an existing instance of the AIS object of it is up to
     * date. If not it creates and returns a new up-to-date instance.
     * 
     * @param session
     */
    @Override
    public AkibaInformationSchema getAis(final Session session) {
        // TODO - make this transactional
        while (true) {
            long wasTimestamp;
            synchronized (this) {
                if (aisTimestamp == saveTimestamp) {
                    return ais;
                }
                wasTimestamp = saveTimestamp;
            }
            AkibaInformationSchema newAis;
            try {
                final StringBuilder sb = new StringBuilder();
                final Map<TableName, Integer> idMap = assembleSchema(session,
                        sb, true, false, false);
                final String schemaText = sb.toString();
                schemaDef = SchemaDef.parseSchema(schemaText);
                newAis = new SchemaDefToAis(schemaDef, true).getAis();
                // Reassign the table ID values.
                for (final Map.Entry<TableName, Integer> entry : idMap
                        .entrySet()) {
                    Table table = newAis.getTable(entry.getKey());
                    table.setTableId(entry.getValue().intValue());
                }
                for (final Map.Entry<TableName, GroupTable> entry : newAis
                        .getGroupTables().entrySet()) {
                    final UserTable root = entry.getValue().getRoot();
                    final Integer rootId = idMap.get(root.getName());
                    assert rootId != null : "Group table with no root!";
                    entry.getValue().setTableId(
                            TreeService.MAX_TABLES_PER_VOLUME - rootId);
                }

            } catch (Exception e) {
                // TODO - better handling
                LOG.error("Exception while building new AIS", e);
                return ais;
            }
            //
            // Detect a race condition in which another schema change happened
            // during creation of the AIS. In that case, simple retry.
            //
            synchronized (this) {
                if (saveTimestamp == wasTimestamp
                        && aisTimestamp != saveTimestamp) {
                    aisTimestamp = saveTimestamp;
                    ais = newAis;
                    final RowDefCache rowDefCache = getRowDefCache();
                    rowDefCache.clear();
                    rowDefCache.setAIS(ais);
                    try {
                        loadTableStatusRecords(session);
                        rowDefCache.fixUpOrdinals(this);
                    } catch (Exception e) {
                        LOG.error("Exception while building new AIS", e);
                    }
                    try {
                        final Store store = serviceManager.getStore();
                        new Writer(new CServerAisTarget(store)).save(newAis);
                    } catch (Exception e) {
                        LOG.warn("Exception while storing AIS tables", e);
                    }
                }
            }
        }
    }

    public AkibaInformationSchema getAisForTests(final String schema)
            throws Exception {
        final StringBuilder sb = new StringBuilder();
        final BufferedReader reader = new BufferedReader(new InputStreamReader(
                CServer.class.getClassLoader().getResourceAsStream(schema)));
        for (String statement : (new MySqlStatementSplitter(reader))) {
            sb.append(statement).append(CServerUtil.NEW_LINE);
        }
        for (final TableDefinition tableStruct : aisSchema) {
            sb.append(tableStruct.getDDL()).append(CServerUtil.NEW_LINE);
        }
        schemaDef = SchemaDef.parseSchema(sb.toString());
        ais = new SchemaDefToAis(schemaDef, true).getAis();
        forceNewTimestamp();
        aisTimestamp = saveTimestamp;
        return ais;
    }

    /**
     * Generates a string version of the entire schema database suitable for the
     * Akiban "schemectomy" operation. The intention is for the returned string
     * to replace all existing akibandb table definitions.
     * 
     * @param session
     * @param withGroupTables
     *            if <code>true</code> this method will define synthetic group
     *            tables to accompany the user table actually defined in the
     *            schema database. The group tables enable Akiban query rewrite
     *            to create queries with fewer joins.
     */
    @Override
    public String schemaString(final Session session,
            final boolean withGroupTables) throws Exception {
        final StringBuilder sb = new StringBuilder();
        assembleSchema(session, sb, true, withGroupTables, true);
        return sb.toString();
    }

    /**
     * Assembles string forms of the Schema in a supplied StringBuilder. This
     * method provides various forms of output for different purposes.
     * 
     * @param session
     * @param sb
     *            The StringBuilder to which statements are written
     * @param withAisTables
     *            <code>true</code> if the akiban_information_schema tables
     *            should be included
     * @param withGroupTables
     *            <code>true</code> if the generated group tables should be
     *            included
     * @param withCreateSchemaStatements
     *            <code>true</code> if create schema statements should be
     *            included.
     * @throws Exception
     */
    private Map<TableName, Integer> assembleSchema(final Session session,
            final StringBuilder sb, final boolean withAisTables,
            final boolean withGroupTables,
            final boolean withCreateSchemaStatements) throws Exception {

        final TreeService treeService = serviceManager.getTreeService();
        final Map<TableName, Integer> idMap = new HashMap<TableName, Integer>();
        // append the AIS table definitions
        if (withAisTables) {
            if (withCreateSchemaStatements) {
                sb.append(CREATE_SCHEMA_IF_NOT_EXISTS);
                TableName.escape(AKIBAN_INFORMATION_SCHEMA, sb);
                sb.append(SEMI_COLON).append(CServerUtil.NEW_LINE);
            }
            for (final TableDefinition td : aisSchema) {
                sb.append(td.getDDL()).append(CServerUtil.NEW_LINE);
                idMap.put(new TableName(td.getSchemaName(), td.getTableName()),
                        td.getTableId());
            }
        }

        // append the User Table definitions
        treeService.visitStorage(session, new TreeVisitor() {

            @Override
            public void visit(Exchange ex) throws Exception {
                ex.clear().append(BY_NAME).append(Key.BEFORE);
                while (ex.next()) {
                    final String schemaName = ex.getKey().indexTo(-1)
                            .decodeString();
                    ex.append(Key.BEFORE);
                    final TreeLink link = treeLink(schemaName, SCHEMA_TREE_NAME);
                    if (treeService.isContainer(ex, link)) {
                        if (withCreateSchemaStatements) {
                            sb.append(CREATE_SCHEMA_IF_NOT_EXISTS);
                            TableName.escape(schemaName, sb);
                            sb.append(SEMI_COLON).append(CServerUtil.NEW_LINE);
                        }
                        while (ex.next()) {
                            final String tableName = ex.getKey().indexTo(-1)
                                    .decodeString();
                            final TableDefinition td = getTableDefinition(
                                    session, schemaName, tableName);
                            sb.append(td.getDDL()).append(CServerUtil.NEW_LINE);
                            int tableId = treeService.storeToAis(link,
                                    td.getTableId());
                            idMap.put(
                                    new TableName(td.getSchemaName(), td
                                            .getTableName()), tableId);
                        }
                    }
                    ex.cut();
                }
            }
        }, SCHEMA_TREE_NAME);

        // append the Group table definitions
        if (withGroupTables) {
            final AkibaInformationSchema ais = getAis(session);
            final List<String> statements = new DDLGenerator()
                    .createAllGroupTables(ais);
            for (final String statement : statements) {
                if (!statement.contains(AKIBAN_INFORMATION_SCHEMA)) {
                    sb.append(statement).append(CServerUtil.NEW_LINE);
                }
            }
        }
        return idMap;
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
    public synchronized long getUpdateTimestamp() {
        return saveTimestamp;
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
        saveTimestamp = treeService.getTimestamp(new SessionImpl());
    }

    private static List<TableDefinition> readAisSchema() {
        List<TableDefinition> definitions = new ArrayList<TableDefinition>();
        int tableId = AIS_BASE_TABLE_ID;
        final BufferedReader reader = new BufferedReader(new InputStreamReader(
                CServer.class.getClassLoader()
                        .getResourceAsStream(AIS_DDL_NAME)));
        final Pattern pattern = Pattern.compile("create table "
                + AKIBAN_INFORMATION_SCHEMA + ".(\\w+).*");
        for (String statement : (new MySqlStatementSplitter(reader))) {
            Matcher matcher = pattern.matcher(statement);
            if (!matcher.find()) {
                throw new RuntimeException("couldn't match regex for: "
                        + statement);
            }
            final String canonical = SchemaDef.canonicalStatement(statement);
            TableDefinition def = new TableDefinition(tableId++,
                    "akiba_information_schema", matcher.group(1), canonical);
            definitions.add(def);
        }
        return Collections.unmodifiableList(definitions);
    }

    @Override
    public SchemaManager cast() {
        return this;
    }

    @Override
    public Class castClass() {
        return SchemaManager.class;
    }

    @Override
    public void start() throws Exception {
        serviceManager = ServiceManagerImpl.get();
        startTableStatusFlusher();
        final Session session = new SessionImpl();
        final TreeService treeService = serviceManager.getTreeService();
        changed(treeService, session);
    }

    @Override
    public void stop() throws Exception {
        this.ais = null;
        this.rowDefCache = null;
        this.serviceManager = null;
        schemaLinkMap.clear();
        statusLinkMap.clear();
        stopTableStatusFlusher();
    }

    /**
     * Create an AIS during the startup process so that the AIS tables are
     * populated and a visible. This can't be done until both this Service and
     * the PersistitStore service are fully initialized and registered, so it is
     * done as an "afterStart" step.
     */
    @Override
    public void afterStart() throws Exception {
        getAis(new SessionImpl());
    }

    /**
     * Load TableStatus records from backing store. This happens only when
     * creating a new AIS due to a schema change or upon system startup.
     * 
     * @param session
     * @throws Exception
     */
    public void loadTableStatusRecords(final Session session)
            throws PersistitException {
        final TreeService treeService = serviceManager.getTreeService();
        for (final RowDef rowDef : getRowDefCache().getRowDefs()) {
            final TableStatus ts = rowDef.getTableStatus();
            final TreeLink link = treeLink(rowDef.getSchemaName(),
                    STATUS_TREE_NAME);
            final Exchange exchange = treeService.getExchange(session, link);
            try {
                int tableId = treeService
                        .aisToStore(link, rowDef.getRowDefId());
                exchange.clear().append(tableId).fetch();
                if (exchange.isValueDefined()) {
                    ts.get(exchange.getValue());
                }
                // Either there is no stored record, or we loaded it. In either
                // case the TableStatus is no longer dirty.
                ts.flushed();
            } finally {
                treeService.releaseExchange(session, exchange);
            }
        }
    }

    /**
     * Remove any TableStatus records belonging to tables that no longer exist.
     * This method should be called by deleteTableDefinition.
     * 
     * @param session
     * @throws PersistitException
     */
    @Override
    public void removeStaleTableStatusRecords(final Session session)
            throws Exception {
        final TreeService treeService = serviceManager.getTreeService();
        treeService.visitStorage(session, new TreeVisitor() {

            @Override
            public void visit(Exchange exchange) throws Exception {
                exchange.clear().to(Key.BEFORE);
                while (exchange.next()) {
                    int tableId = exchange.getKey().reset().decodeInt();
                    tableId = treeService.storeToAis(exchange.getVolume(),
                            tableId);
                    final RowDef rowDef = getRowDefCache().rowDef(tableId);
                    if (rowDef == null) {
                        exchange.remove();
                    }
                }
            }
        }, STATUS_TREE_NAME);
    }

    @Override
    public void saveTableStatusRecords(final Session session)
            throws PersistitException {
        final TreeService treeService = serviceManager.getTreeService();
        for (final RowDef rowDef : getRowDefCache().getRowDefs()) {
            final TableStatus ts = rowDef.getTableStatus();
            if (ts.isDirty()) {
                final TreeLink link = treeLink(rowDef.getSchemaName(),
                        STATUS_TREE_NAME);
                final Exchange exchange = treeService
                        .getExchange(session, link);
                try {
                    final int tableId = treeService.aisToStore(link,
                            rowDef.getRowDefId());
                    exchange.clear().append(tableId);
                    ts.put(exchange.getValue());
                    exchange.store();
                    ts.flushed();
                } finally {
                    treeService.releaseExchange(session, exchange);
                }
            }
        }
    }

    void saveStatus(final Session session, final TableStatus tableStatus)
            throws PersistitException {
        final TreeService treeService = serviceManager.getTreeService();
        final RowDef rowDef = getRowDefCache().getRowDef(
                tableStatus.getRowDefId());
        final TreeLink link = treeLink(rowDef.getSchemaName(), STATUS_TREE_NAME);
        final Exchange exchange = treeService.getExchange(session, link);
        try {
            final int tableId = treeService.aisToStore(link,
                    rowDef.getRowDefId());
            exchange.clear().append(tableId);
            tableStatus.put(exchange.getValue());
            exchange.store();
            tableStatus.flushed();
        } finally {
            treeService.releaseExchange(session, exchange);
        }
    }

    private TreeLink treeLink(final String schemaName, final String treeName) {
        final Map<String, TreeLink> map = treeName == STATUS_TREE_NAME ? statusLinkMap
                : schemaLinkMap;
        TreeLink link;
        synchronized (map) {
            link = map.get(schemaName);
            if (link == null) {
                link = new TreeLink() {
                    TreeCache cache;

                    @Override
                    public String getSchemaName() {
                        return schemaName;
                    }

                    @Override
                    public String getTreeName() {
                        return treeName;
                    }

                    @Override
                    public void setTreeCache(TreeCache cache) {
                        this.cache = cache;
                    }

                    @Override
                    public TreeCache getTreeCache() {
                        return cache;
                    }

                };
                map.put(schemaName, link);
            }
        }
        return link;
    }

    static long now() {
        return System.nanoTime() / 1000L;
    }

    private RowDefCache getRowDefCache() {
        if (rowDefCache == null) {
            rowDefCache = serviceManager.getStore().getRowDefCache();
        }
        return rowDefCache;
    }

    private SchemaDef.UserTableDef parseTableStatement(
            final String defaultSchemaName, final String canonical)
            throws InvalidOperationException {
        try {
            return new SchemaDef().parseCreateTable(canonical);
        } catch (Exception e1) {
            throw new InvalidOperationException(ErrorCode.PARSE_EXCEPTION,
                    String.format("[%s] %s: %s", defaultSchemaName, e1.getMessage(), canonical),
                    e1);
        }
    }

    private SchemaDef.CName cname(final String canonical)
            throws InvalidOperationException {
        final SchemaDef.UserTableDef def = parseTableStatement(null, canonical);
        final SchemaDef.CName cname = def.getCName();
        if (cname == null || cname.getSchema() == null
                || cname.getName() == null) {
            throw new InvalidOperationException(ErrorCode.PARSE_EXCEPTION,
                    "[%s] %s: %s", cname.getSchema(),
                    "Null schema or table name", canonical);
        }
        return cname;
    }

    private void validateTableDefinition(final Session session,
            final String schemaName, final String statement,
            final SchemaDef.UserTableDef tableDef) throws Exception {

        if (AKIBAN_INFORMATION_SCHEMA.equals(tableDef.getCName().getSchema())) {
            throw new InvalidOperationException(ErrorCode.PROTECTED_TABLE,
                    "[%s] %s is protected: %s", schemaName,
                    AKIBAN_INFORMATION_SCHEMA, statement);
        }

        final SchemaDef.IndexDef parentJoin = SchemaDef.getAkibanJoin(tableDef);
        if (parentJoin == null) {
            return;
        }

        String parentSchema = parentJoin.getParentSchema();
        if (parentSchema == null) {
            parentSchema = (tableDef.getCName().getSchema() == null) ? schemaName
                    : tableDef.getCName().getSchema();
        }

        if (AKIBAN_INFORMATION_SCHEMA.equals(parentSchema)) {
            throw new InvalidOperationException(
                    ErrorCode.JOIN_TO_PROTECTED_TABLE, "[%s] to %s.*: %s",
                    schemaName, parentJoin.getParentSchema(), statement);
        }

        final String tableName = tableDef.getCName().getName();
        final String parentTableName = parentJoin.getParentTable();
        if (schemaName.equals(parentSchema)
                && tableName.equals(parentTableName)) {
            throw new InvalidOperationException(
                    ErrorCode.JOIN_TO_UNKNOWN_TABLE,
                    "Table %s.%s refers to undefined table %s.%s: %s",
                    schemaName, tableName, parentSchema, parentTableName,
                    statement);
        }
        final TableDefinition parentDef = getTableDefinition(session,
                parentSchema, parentTableName);
        if (parentDef == null) {
            throw new InvalidOperationException(
                    ErrorCode.JOIN_TO_UNKNOWN_TABLE,
                    "Table %s.%s refers to undefined table %s.%s: %s",
                    schemaName, tableName, parentSchema, parentTableName,
                    statement);
        }
        final SchemaDef.UserTableDef parentTableDef = new SchemaDef()
                .parseCreateTable(parentDef.getDDL());
        for (final String columnName : parentJoin.getParentColumns()) {
            if (!parentTableDef.getColumnNames().contains(columnName)) {
                throw new InvalidOperationException(
                        ErrorCode.JOIN_TO_WRONG_COLUMNS,
                        "Table %s.%s refers to undefined column %s in table %s.%s: %s",
                        schemaName, tableName, columnName, parentSchema,
                        parentTableName, statement);
            }
        }
    }

    private void changed(final TreeService treeService, final Session session) {
        synchronized (this) {
            // TODO - this is good enough for now (mostly single-threaded)
            // but when we have transactional cache, should use that
            // instead.
            saveTimestamp = Math.max(saveTimestamp,
                    treeService.getTimestamp(session));
        }
    }

    private static String naked(final String canonical) {
        return canonical.substring(CREATE_TABLE.length());
    }

    /**
     * Start a Timer that periodically flushes any changed TableStatus records
     * to backing store. TODO: remove this and instead implement
     * "transactional cache".
     * 
     * @throws Exception
     */
    private void startTableStatusFlusher() throws Exception {
        //
        // Schedule Timer to flush every DELAY milliseconds.
        //
        final Session session = new SessionImpl();
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                try {
                    saveTableStatusRecords(session);
                } catch (Exception e) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Failed to updateTableState", e);
                    }
                }
            }
        }, DELAY, DELAY);
    }

    private void stopTableStatusFlusher() {
        timer.cancel();
    }
}
