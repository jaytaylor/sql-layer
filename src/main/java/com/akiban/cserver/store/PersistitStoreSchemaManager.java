package com.akiban.cserver.store;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.ddl.SchemaDef;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.cserver.CServer;
import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.StorageLink;
import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.ServiceManager;
import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.cserver.service.persistit.PersistitService;
import com.akiban.cserver.service.persistit.StorageVisitor;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.message.ErrorCode;
import com.akiban.util.MySqlStatementSplitter;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.exception.PersistitException;

public class PersistitStoreSchemaManager implements Service<SchemaManager>,
        SchemaManager {
    private static final Log LOG = LogFactory
            .getLog(PersistitStoreSchemaManager.class.getName());

    private final static String CREATE_TABLE = "create table ";

    private final static String CREATE_SCHEMA_IF_NOT_EXISTS = "create schema if not exists ";

    private final static int AIS_BASE_TABLE_IDS = 100000;

    private static final int GROUP_TABLE_ID_OFFSET = 1000000000;

    private static final String AIS_DDL_NAME = "akiba_information_schema.ddl";

    private final static String SCHEMA_TREE_NAME = "_schema_";

    private final static String AKIBAN_INFORMATION_SCHEMA = "akiba_information_schema";

    private final static String BY_ID = "byId";

    private final static String BY_NAME = "byName";

    private AkibaInformationSchema ais;

    private long aisTimestamp;

    private List<TableDefinition> aisSchema;

    private ServiceManager serviceManager;

    private long timestamp;

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
            final String defaultSchemaName, final String statement)
            throws Exception {
        final PersistitService ps = serviceManager.getPersistitService();
        String canonical = DDLSource.canonicalStatement(statement);
        final SchemaDef.UserTableDef tableDef = parseTableStatement(
                defaultSchemaName, canonical);
        String schemaName = tableDef.getCName().getSchema();
        if (schemaName == null) {
            final StringBuilder sb = new StringBuilder();
            schemaName = defaultSchemaName;
            TableName.escape(schemaName, sb);
            sb.append(".");
            sb.append(canonical);
            canonical = sb.toString();
        }
        final String tableName = tableDef.getCName().getName();

        validateTableDefinition(schemaName, statement, tableDef);

        final StorageLink storageLink = storageLink(schemaName);
        final Exchange ex = ps.getExchange(session, storageLink);

        try {
            if (ex.clear().append(BY_NAME).append(schemaName).append(tableName)
                    .append(Key.AFTER).previous()) {
                final int tableId = ex.getKey().indexTo(-1).decodeInt();
                ex.clear().append(BY_ID).append(tableId).fetch();
                final String previousValue = ex.getValue().getString();
                if (canonical.equals(previousValue)) {
                    return;
                }
            }

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
            changed(ps, session);
            return;
        } finally {
            ps.releaseExchange(session, ex);
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
        final PersistitService ps = serviceManager.getPersistitService();
        ps.visitStorage(new StorageVisitor() {

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
        final StorageLink storageLink = storageLink(schemaName);
        final PersistitService ps = serviceManager.getPersistitService();
        Exchange ex1 = null;
        Exchange ex2 = null;
        try {
            ex1 = ps.getExchange(session, storageLink);
            ex2 = ps.getExchange(session, storageLink);
            ex1.clear().append(BY_NAME).append(schemaName).append(tableName);
            final KeyFilter keyFilter = new KeyFilter(ex1.getKey(), 4, 4);

            ex1.clear();
            while (ex1.next(keyFilter)) {
                final int tableId = ex1.getKey().indexTo(-1).decodeInt();
                ex2.clear().append(BY_ID).append(tableId).remove();
                ex1.remove();
            }
            changed(ps, session);
        } catch (PersistitException e) {
            LOG.error("Failed to delete table " + schemaName + "." + tableName,
                    e);
            throw e;
        } finally {
            if (ex1 != null) {
                ps.releaseExchange(session, ex1);
            }
            if (ex2 != null) {
                ps.releaseExchange(session, ex2);
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
     * @param tableName
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
        final StorageLink storageLink = storageLink(schemaName);
        final PersistitService ps = serviceManager.getPersistitService();
        Exchange ex1 = null;
        Exchange ex2 = null;
        try {
            ex1 = ps.getExchange(session, storageLink);
            ex2 = ps.getExchange(session, storageLink);
            ex1.clear().append(BY_NAME).append(schemaName);
            final KeyFilter keyFilter = new KeyFilter(ex1.getKey(), 4, 4);

            ex1.clear();
            while (ex1.next(keyFilter)) {
                final int tableId = ex1.getKey().indexTo(-1).decodeInt();
                ex2.clear().append(BY_ID).append(tableId).remove();
                ex1.remove();
            }
            changed(ps, session);
        } catch (PersistitException e) {
            LOG.error("Failed to delete schema " + schemaName, e);
            throw e;
        } finally {
            if (ex1 != null) {
                ps.releaseExchange(session, ex1);
            }
            if (ex2 != null) {
                ps.releaseExchange(session, ex2);
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
        final PersistitService ps = serviceManager.getPersistitService();
        ps.visitStorage(new StorageVisitor() {

            @Override
            public void visit(final Exchange exchange) throws Exception {
                exchange.clear().removeAll();
            }

        }, SCHEMA_TREE_NAME);
        changed(ps, session);
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
        final StorageLink storageLink = storageLink(schemaName);
        final PersistitService ps = serviceManager.getPersistitService();
        final Exchange ex = ps.getExchange(session, storageLink);
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
            ps.releaseExchange(session, ex);
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
        final StorageLink storageLink = storageLink(schemaName);
        final PersistitService ps = serviceManager.getPersistitService();
        final Exchange ex = ps.getExchange(session, storageLink);
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
            ps.releaseExchange(session, ex);
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
        final StorageLink storageLink = storageLink(schemaName);
        final PersistitService ps = serviceManager.getPersistitService();
        final Exchange ex = ps.getExchange(session, storageLink);
        ex.clear().append(BY_NAME).append(schemaName);
        final KeyFilter kf = new KeyFilter(ex.getKey(), 3, 3);
        try {
            while (ex.next(kf)) {
                final String tableName = ex.getKey().indexTo(-1).decodeString();
                if (ex.append(Key.AFTER).previous()) {
                    final int tableId = ex.getKey().indexTo(-1).decodeInt();
                    ex.clear().append(BY_ID).append(tableId).fetch();
                    result.put(tableName, new TableDefinition(tableId,
                            schemaName, tableName, ex.getValue().getString()));
                }
            }
            return result;
        } finally {
            ps.releaseExchange(session, ex);
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
            final PersistitService ps = serviceManager.getPersistitService();
            long wasTimestamp;
            synchronized (this) {
                if (aisTimestamp == timestamp) {
                    return ais;
                }
                wasTimestamp = timestamp;
            }
            AkibaInformationSchema newAis;
            try {
                final String schemaText = schemaString(session, false);
                newAis = new DDLSource().buildAISFromString(schemaText);
            } catch (Exception e) {
                LOG.error("Exception while building new AIS", e);
                return ais;
            }
            synchronized (this) {
                if (timestamp == wasTimestamp && aisTimestamp != timestamp) {
                    aisTimestamp = timestamp;
                    ais = newAis;
                }
            }
        }
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
        assembleSchema(session, sb, true, withGroupTables, false);
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
    private void assembleSchema(final Session session, final StringBuilder sb,
            final boolean withAisTables, final boolean withGroupTables,
            final boolean withCreateSchemaStatements) throws Exception {

        final PersistitService ps = serviceManager.getPersistitService();
        
        // append the AIS table definitions
        if (withAisTables) {
            if (withCreateSchemaStatements) {
                sb.append(CREATE_SCHEMA_IF_NOT_EXISTS);
                TableName.escape(AKIBAN_INFORMATION_SCHEMA, sb);
                sb.append(CServerUtil.NEW_LINE);
            }
            for (final TableDefinition tableStruct : aisSchema) {
                sb.append(CREATE_TABLE).append(tableStruct.getDDL())
                        .append(CServerUtil.NEW_LINE);
            }
        }
        
        // append the User Table definitions
        ps.visitStorage(new StorageVisitor() {

            @Override
            public void visit(Exchange ex) throws Exception {
                ex.clear().append(BY_NAME).append(Key.BEFORE);
                while (ex.next()) {
                    final String schemaName = ex.getKey().indexTo(-1)
                            .decodeString();
                    ex.append(Key.BEFORE);
                    if (ps.isContainer(ex, storageLink(schemaName))) {
                        if (withCreateSchemaStatements) {
                            sb.append(CREATE_SCHEMA_IF_NOT_EXISTS);
                            TableName.escape(schemaName, sb);
                            sb.append(CServerUtil.NEW_LINE);
                        }
                        while (ex.next()) {
                            final String tableName = ex.getKey().indexTo(-1)
                                    .decodeString();
                            final TableDefinition td = getTableDefinition(
                                    session, schemaName, tableName);
                            sb.append(CREATE_TABLE).append(td.getDDL())
                                    .append(CServerUtil.NEW_LINE);
                        }
                    }
                    ex.cut();
                }
            }
        }, SCHEMA_TREE_NAME);
        
        // append the Group table definitions
        if (withGroupTables) {
            final AkibaInformationSchema ais = getAis(session);
            final List<String> statements = new DDLGenerator().createAllGroupTables(ais);
            for (final String statement : statements) {
                sb.append(statement).append(CServerUtil.NEW_LINE);
            }
        }

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
        return timestamp;
    }

    @Override
    public int getSchemaGeneration() {
        final long ts = getUpdateTimestamp();
        return (int)ts ^ (int)(ts >>> 32);
    }
    /**
     * Updates the current timestamp to a new value greater than any previously
     * returned value. This method can be used to force clients that rely on a
     * timestamp value to determine staleness to refresh their cached schema
     * data.
     */
    @Override
    public synchronized void forceNewTimestamp() {
        final PersistitService ps = serviceManager.getPersistitService();
        timestamp = ps.getTimestamp(new SessionImpl());
    }

    private List<TableDefinition> readAisSchema() {
        List<TableDefinition> definitions = new ArrayList<TableDefinition>();
        int tableId = AIS_BASE_TABLE_IDS;
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
            final String canonical = DDLSource.canonicalStatement(statement);
            TableDefinition def = new TableDefinition(tableId++,
                    "akiba_information_schema", matcher.group(1), canonical);
            definitions.add(def);
        }
        return Collections.unmodifiableList(definitions);
    }

    private StorageLink storageLink(final String schemaName) {
        return new StorageLink() {
            Object cache;

            @Override
            public String getSchemaName() {
                return schemaName;
            }

            @Override
            public String getTableName() {
                return null;
            }

            @Override
            public String getIndexName() {
                return null;
            }

            @Override
            public String getTreeName() {
                return SCHEMA_TREE_NAME;
            }

            @Override
            public void setStorageCache(Object object) {
                cache = object;
            }

            @Override
            public Object getStorageCache() {
                return cache;
            }
        };
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
        this.serviceManager = ServiceManagerImpl.get();
        this.aisSchema = readAisSchema();
        changed(serviceManager.getPersistitService(), new SessionImpl());
    }

    @Override
    public void stop() throws Exception {
        // Nothing to do

    }

    private SchemaDef.UserTableDef parseTableStatement(
            final String defaultSchemaName, final String canonical)
            throws InvalidOperationException {
        try {
            return new DDLSource().parseCreateTable(canonical);
        } catch (Exception e1) {
            throw new InvalidOperationException(ErrorCode.PARSE_EXCEPTION,
                    "[%s] %s: %s", defaultSchemaName, e1.getMessage(),
                    canonical);
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

    private void validateTableDefinition(final String schemaName,
            final String statement, final SchemaDef.UserTableDef tableDef)
            throws InvalidOperationException {

        if (AKIBAN_INFORMATION_SCHEMA.equals(tableDef.getCName().getSchema())) {
            throw new InvalidOperationException(ErrorCode.PROTECTED_TABLE,
                    "[%s] %s is protected: %s", schemaName,
                    AKIBAN_INFORMATION_SCHEMA, statement);
        }
        if (tableDef.getPrimaryKey().size() == 0) {
            throw new InvalidOperationException(ErrorCode.NO_PRIMARY_KEY,
                    "[%s] %s", schemaName, statement);
        }

        SchemaDef.IndexDef parentJoin = DDLSource.getAkibanJoin(tableDef);
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

        // TODO - make sure the referenced table and columns exist
    }

    private void changed(final PersistitService ps, final Session session) {
        synchronized (this) {
            // TODO - this is good enough for now (mostly single-threaded)
            // but when we have transactional cache, should use that
            // instead.
            timestamp = Math.max(timestamp, ps.getTimestamp(session));
        }

    }

}
