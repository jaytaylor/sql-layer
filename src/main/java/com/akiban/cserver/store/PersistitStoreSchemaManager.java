package com.akiban.cserver.store;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.ddl.SchemaDef;
import com.akiban.ais.io.Writer;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.staticgrouping.*;
import com.akiban.ais.util.AISPrinter;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.cserver.*;
import com.akiban.cserver.manage.SchemaManager;
import com.akiban.message.ErrorCode;
import com.akiban.util.MySqlStatementSplitter;
import com.persistit.*;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PersistitStoreSchemaManager implements CServerConstants,
        SchemaManager {

    private static final Log LOG = LogFactory
            .getLog(PersistitStoreSchemaManager.class.getName());

    private final static int AIS_BASE_TABLE_IDS = 100000;

    private static final String AIS_DDL_NAME = "akiba_information_schema.ddl";

    private static final int GROUP_TABLE_ID_OFFSET = 1000000000;

    private final static String SCHEMA_TREE_NAME = "_schema_";

    private final static String AKIBA_INFORMATION_SCHEMA = "akiba_information_schema";

    private final static String BY_ID = "byId";

    private final static String BY_NAME = "byName";

    private final PersistitStore store;

    private final Set<ColumnName> knownColumns = new HashSet<ColumnName>(100);

    // TODO -Initialize as an empty AIS - needed for a unit test
    private AkibaInformationSchema ais;

    private List<CreateTableStruct> aisSchemaStructs;

    private List<String> aisSchemaDdls;

    public void startUp() throws Exception {
        acquireAIS();
    }

    private static class ColumnName {
        private final String tableName;
        private final String columnName;

        private ColumnName(String tableName, String columnName) {
            assert tableName != null : "null tablename";
            assert columnName != null : "null columnName";
            this.tableName = tableName;
            this.columnName = columnName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ColumnName that = (ColumnName) o;
            if (!columnName.equals(that.columnName)) {
                return false;
            }
            if (!tableName.equals(that.tableName)) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = tableName.hashCode();
            result = 31 * result + columnName.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return String.format("(%s,%s)", tableName, columnName);
        }
    }

    public static class CreateTableStruct {
        final int tableId;
        final String schemaName;
        final String tableName;
        final String ddl;

        public CreateTableStruct(int tableId, String schemaName,
                String tableName, String ddl) {
            this.tableId = tableId;
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.ddl = ddl;
        }

        public String getDdl() {
            return ddl;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public String getTableName() {
            return tableName;
        }

        public int getTableId() {
            return tableId;
        }

        @Override
        public String toString() {
            return "CreateTableStruct[" + tableId + ": "
                    + TableName.create(schemaName, tableName) + ']';
        }
    }

    public PersistitStoreSchemaManager(final PersistitStore store) {
        this.store = store;
        this.aisSchemaDdls = readAisDdls();
        this.aisSchemaStructs = readAisSchemaDdls(aisSchemaDdls);
        this.ais = createEmptyAIS();
    }

    private void populateSchema(final List<CreateTableStruct> result)
            throws PersistitException {
        Exchange ex1 = null;
        Exchange ex2 = null;

        try {
            ex1 = store.getExchange(SCHEMA_TREE_NAME);
            ex2 = store.getExchange(SCHEMA_TREE_NAME);
            ex1.clear().append(BY_NAME);
            final KeyFilter keyFilter = new KeyFilter(ex1.getKey(), 4, 4);

            ex1.clear();
            while (ex1.next(keyFilter)) {
                // Traverse to the largest tableId (most recent)
                if (!ex1.to(Key.AFTER).previous()) {
                    continue;
                }
                final String schemaName = ex1.getKey().indexTo(1)
                        .decodeString();
                final String tableName = ex1.getKey().indexTo(2).decodeString();
                final int tableId = ex1.getKey().indexTo(3).decodeInt();

                ex2.clear().append(BY_ID).append(tableId).fetch();
                if (!ex2.getValue().isDefined()) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("No table definition for " + ex1.getKey());
                    }
                }
                final String ddl = ex2.getValue().getString();
                result.add(new CreateTableStruct(tableId, schemaName,
                        tableName, ddl));
            }

        } catch (Throwable t) {
            LOG.error("createTable failed", t);
        } finally {
            store.releaseExchange(ex1);
            store.releaseExchange(ex2);
        }
    }

    private boolean checkForDuplicateColumns(SchemaDef.UserTableDef tableDef) {
        Set<ColumnName> tmp = new HashSet<ColumnName>(knownColumns);
        for (SchemaDef.ColumnDef cDef : tableDef.getColumns()) {
            ColumnName cName = new ColumnName(tableDef.getCName().getName(),
                    cDef.getName());
            if (!tmp.add(cName)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("table/column pair already exists: " + cName
                            + " -- abandoning createTable");
                }
                return false;
            }
        }
        knownColumns.addAll(tmp);
        assert knownColumns.size() == tmp.size() : String.format(
                "union not of equal size: %s after adding %s", knownColumns,
                tmp);
        return true;
    }

    public void forgetTableColumns(String tableName) {
        Iterator<ColumnName> iter = knownColumns.iterator();
        while (iter.hasNext()) {
            if (tableName.equals(iter.next().tableName)) {
                iter.remove();
            }
        }
    }

    /**
     * Attempts to create a table.
     * 
     * @param useSchemaName
     *            the table's schema name
     * @param ddl
     *            the table's raw DDL
     * @param rowDefCache
     *            the existing RowDefCache. Used to validate parent columns.
     * @param result
     *            the result object to be populated
     * @return CServerConstants.OK iff everything worked
     * @throws InvalidOperationException
     *             if the table isn't valid (or can't be parsed)
     */
    void createTable(final String useSchemaName, final String ddl, RowDefCache rowDefCache, CreateTableResult result)
            throws InvalidOperationException, PersistitException {
        Exchange ex = null;

        String canonical = DDLSource.canonicalStatement(ddl);
        final SchemaDef.UserTableDef tableDef;
        try {
            tableDef = new DDLSource().parseCreateTable(canonical);
        } catch (Exception e1) {
            throw new InvalidOperationException(ErrorCode.PARSE_EXCEPTION,
                    "[%s] %s: %s", useSchemaName, e1.getMessage(), canonical);
        }
        if (AKIBA_INFORMATION_SCHEMA.equals(tableDef.getCName().getSchema())
                || "akiba_objects".equals(tableDef.getCName().getSchema())) {
            throw new InvalidOperationException(ErrorCode.PROTECTED_TABLE,
                    "[%s] %s is protected: %s", useSchemaName,
                    AKIBA_INFORMATION_SCHEMA, ddl);
        }
        SchemaDef.IndexDef parentJoin = DDLSource.getAkibanJoin(tableDef);
        if (parentJoin != null) {
            if (AKIBA_INFORMATION_SCHEMA.equals(parentJoin.getParentSchema())
                    || "akiba_objects".equals(parentJoin.getParentSchema())) {
                throw new InvalidOperationException(
                        ErrorCode.JOIN_TO_PROTECTED_TABLE, "[%s] to %s.*: %s",
                        useSchemaName, parentJoin.getParentSchema(), ddl);
            }
            String parentSchema = parentJoin.getParentSchema();
            if (parentSchema == null) {
                parentSchema = (tableDef.getCName().getSchema() == null) ? useSchemaName
                        : tableDef.getCName().getSchema();
            }
            final String parentName = RowDefCache.nameOf(
                    parentJoin.getParentSchema(useSchemaName),
                    parentJoin.getParentTable());
            final RowDef parentDef = rowDefCache.getRowDef(parentName);
            if (parentDef == null) {
                throw new InvalidOperationException(
                        ErrorCode.JOIN_TO_UNKNOWN_TABLE, "[%s] to %s: %s",
                        useSchemaName, parentName, ddl);
            }
            IndexDef parentPK = parentDef.getPKIndexDef();
            List<String> parentPKColumns = new ArrayList<String>(
                    parentPK.getFields().length);
            for (int fieldIndex : parentPK.getFields()) {
                parentPKColumns
                        .add(parentDef.getFieldDef(fieldIndex).getName());
            }
            if (!parentPKColumns.equals(parentJoin.getParentColumns())) {
                throw new InvalidOperationException(
                        ErrorCode.JOIN_TO_WRONG_COLUMNS,
                        "children must join to parent's PK columns. [%s] %s%s references %s%s: %s", useSchemaName,
                        tableDef.getCName(), parentJoin.getParentColumns(),
                        parentName, parentPKColumns, ddl);
            }
        }

        // TODO: For now, we can't handle situations in which a
        // tablename+columnname already exist
        // This is because group table columns are qualified only by
        // (uTable,uTableCol) so there
        // is a collision if we have (s1, tbl, col) and (s2, tbl, col)
        if (!checkForDuplicateColumns(tableDef)) {
            throw new InvalidOperationException(
                    ErrorCode.DUPLICATE_COLUMN_NAMES,
                    "[%s] knownColumns=%s  ddl=%s", useSchemaName,
                    knownColumns, ddl);
        }

        try {
            ex = store.getExchange(SCHEMA_TREE_NAME);

            String schemaName = tableDef.getCName().getSchema();
            if (schemaName == null) {
                schemaName = useSchemaName;
                canonical = '`' + useSchemaName + "`." + canonical;
            }
            final String tableName = tableDef.getCName().getName();

            if (ex.clear().append(BY_NAME).append(schemaName).append(tableName)
                    .append(Key.AFTER).previous()) {
                final int tableId = ex.getKey().indexTo(-1).decodeInt();
                ex.clear().append(BY_ID).append(tableId).fetch();
                final String previousValue = ex.getValue().getString();
                if (canonical.equals(previousValue)) {
                    if (result != null) {
                        result.fillInInfo(tableDef, tableId);
                    }
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

            if (result != null) {
                result.fillInInfo(tableDef, tableId);
            }
            return;

            // } catch (StoreException e) {
            // if (verbose && LOG.isInfoEnabled()) {
            // LOG.info("createTable error " + e.getResult(), e);
            // }
            // return e.getResult();
        } finally {
            store.releaseExchange(ex);
        }
    }

    /**
     * Removes the create table statement(s) for the specified schema/table
     * 
     * @param schemaName
     *            the table's schema
     * @param tableName
     *            the table's name
     * @throws InvalidOperationException
     *             if the table is protected
     */
    void dropCreateTable(final String schemaName, final String tableName)
            throws PersistitException {

        if (AKIBA_INFORMATION_SCHEMA.equals(schemaName)
                || "akiba_objects".equals(schemaName)) {
            return;
        }

        Exchange ex1 = null;
        Exchange ex2 = null;
        try {
            ex1 = store.getExchange(SCHEMA_TREE_NAME);
            ex2 = store.getExchange(SCHEMA_TREE_NAME);
            ex1.clear().append(BY_NAME).append(schemaName).append(tableName);
            final KeyFilter keyFilter = new KeyFilter(ex1.getKey(), 4, 4);

            ex1.clear();
            while (ex1.next(keyFilter)) {
                final int tableId = ex1.getKey().indexTo(-1).decodeInt();
                ex2.clear().append(BY_ID).append(tableId).remove();
                ex1.remove();
            }
        } catch (PersistitException e) {
            LOG.error(TableName.create(schemaName, tableName).toString(), e);
            throw e;
        } finally {
            if (ex1 != null) {
                store.releaseExchange(ex1);
            }
            if (ex2 != null) {
                store.releaseExchange(ex2);
            }
        }
    }

    private List<CreateTableStruct> getSchemaStructs() throws Exception {
        Transaction transaction = store.getDb().getTransaction();
        final List<CreateTableStruct> result = new ArrayList<CreateTableStruct>();
        transaction.run(new TransactionRunnable() {

            @Override
            public void runTransaction() throws PersistitException,
                    RollbackException {
                populateSchema(result);
            }
        });
        return result;
    }

    /**
     * Intended for testing. Creates a new AIS with only the a_i_s tables as its
     * user tables.
     * 
     * @return a new AIS instance
     * @throws Exception
     *             if there's a problem
     */
    public AkibaInformationSchema createEmptyAIS() {
        try {
            return createFreshAIS(new ArrayList<CreateTableStruct>());
        } catch (Exception e) {
            // TODO - I suspect this mechanism for pre-loading the AIS
            // may not last, so this is a sloppy temporary thing.
            // DDLSource shouldn't throw anything on an empty schema.
            //
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a fresh AIS, using as its source the default AIS schema DDLs as
     * well as any stored schemata.
     * 
     * @param schema
     *            the stored schema to add to the default AIS schema.
     * @return a new AIS
     * @throws Exception
     *             if there's a problem
     */
    private AkibaInformationSchema createFreshAIS(
            final List<CreateTableStruct> schema) throws Exception {
        schema.addAll(0, aisSchemaStructs);
        assert !schema.isEmpty() : "schema list is empty";
        final StringBuilder sb = new StringBuilder();
        for (final CreateTableStruct tableStruct : schema) {
            sb.append("CREATE TABLE ").append(tableStruct.ddl)
                    .append(CServerUtil.NEW_LINE);
        }
        final String schemaText = sb.toString();
        if (store.isVerbose() && LOG.isInfoEnabled()) {
            LOG.info("Acquiring AIS from schema: " + CServerUtil.NEW_LINE
                    + schemaText);
        }

        AkibaInformationSchema ret = new DDLSource()
                .buildAISFromString(schemaText);
        for (final CreateTableStruct tableStruct : schema) {
            final UserTable table = ret.getUserTable(tableStruct.schemaName,
                    tableStruct.tableName);
            assert table != null : tableStruct + " in " + schema;
            assert table.getGroup() != null : "table "
                    + table
                    + " has no group; should be in a single-table group at least";
            table.setTableId(tableStruct.tableId);
            if (table.getParentJoin() == null) {
                final GroupTable groupTable = table.getGroup().getGroupTable();
                if (groupTable != null) {
                    groupTable.setTableId(tableStruct.tableId
                            + GROUP_TABLE_ID_OFFSET);
                }
            }
        }
        return ret;
    }

    /**
     * Acquire an AkibaInformationSchema from MySQL and install it into the
     * local RowDefCache.
     * 
     * This method always refreshes the locally cached AkibaInformationSchema to
     * support schema modifications at the MySQL head.
     * 
     * @return an AkibaInformationSchema
     * @throws Exception
     */
    public synchronized void acquireAIS() throws Exception {
        if (!store.isExperimentalSchema()) {
            throw new UnsupportedOperationException(
                    "non-experimental mode is deprecated.");
        }

        this.ais = createFreshAIS(getSchemaStructs());
        installAIS();
        new Writer(new CServerAisTarget(store)).save(ais);
    }

    private synchronized void installAIS() throws Exception {
        if (LOG.isInfoEnabled()) {
            LOG.info("Installing " + ais.getDescription() + " in ChunkServer");
            LOG.debug(AISPrinter.toString(ais));
        }
        final RowDefCache rowDefCache = store.getRowDefCache();
        rowDefCache.clear();
        rowDefCache.setAIS(ais);
        store.fixUpOrdinals();
        if (false) {
            // TODO: Use this when we support multiple chunkservers
            // if (isLeader()) {
            // assert aisDistributor != null;
            // aisDistributor.distribute(ais);
            // }
        }
    }

    private List<CreateTableStruct> readAisSchemaDdls(List<String> ddls) {
        final Pattern regex = Pattern.compile("create table (\\w+)",
                Pattern.CASE_INSENSITIVE);
        List<CreateTableStruct> tmp = new ArrayList<CreateTableStruct>();
        int tableId = AIS_BASE_TABLE_IDS;
        for (String ddl : ddls) {
            Matcher matcher = regex.matcher(ddl);
            if (!matcher.find()) {
                throw new RuntimeException("couldn't match regex for: " + ddl);
            }
            String hackedDdl = "`akiba_information_schema`." + matcher.group(1)
                    + ddl.substring(matcher.end());
            CreateTableStruct struct = new CreateTableStruct(tableId++,
                    "akiba_information_schema", matcher.group(1), hackedDdl);
            tmp.add(struct);
        }
        return Collections.unmodifiableList(tmp);
    }

    private List<String> readAisDdls() {
        final List<String> ret = new ArrayList<String>();
        final BufferedReader reader = new BufferedReader(new InputStreamReader(
                CServer.class.getClassLoader()
                        .getResourceAsStream(AIS_DDL_NAME)));
        for (String ddl : (new MySqlStatementSplitter(reader))) {
            ret.add(ddl);
        }
        return Collections.unmodifiableList(ret);
    }

    public AkibaInformationSchema getAis() {
        return ais;
    }

    /**
     * Returns an unmodifiable list of AIS DDLs.
     * 
     * @return
     */
    public List<String> getAisDdls() {
        return aisSchemaDdls;
    }

    /**
     * Gets a copy of the AIS.
     * @return a copy of the currently installed AIS, minus all a_i_s.* tables
     *         and their associated group tables.
     */
    public AkibaInformationSchema getAisCopy() {
        //assert getAisCopyCallerIsOkay();
        AkibaInformationSchema ret = new AkibaInformationSchema(ais);
        List<TableName> uTablesToRemove = new ArrayList<TableName>();
        for (Map.Entry<TableName, UserTable> entries : ret.getUserTables()
                .entrySet()) {
            TableName tableName = entries.getKey();
            if (tableName.getSchemaName().equals("akiba_information_schema")
                    || tableName.getSchemaName().equals("akiba_objects")) {
                uTablesToRemove.add(tableName);
                UserTable uTable = entries.getValue();
                com.akiban.ais.model.Group group = uTable.getGroup();
                if (group != null) {
                    ret.getGroups().remove(group.getName());
                    ret.getGroupTables()
                            .remove(group.getGroupTable().getName());
                }
            }
        }
        for (TableName removeMe : uTablesToRemove) {
            ret.getUserTables().remove(removeMe);
        }
        return ret;
    }

    /**
     * Asserts that the caller of getAisCopy()
     * 
     * @return <tt>true</tt>, always; if there's an error, this method will
     *         raise the assertion.
     */
    private static boolean getAisCopyCallerIsOkay() {
        StackTraceElement callerStack = Thread.currentThread().getStackTrace()[2];
        final Class<?> callerClass;
        try {
            callerClass = Class.forName(callerStack.getClassName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("couldn't load class "
                    + callerStack.getClassName(), e);
        }
        assert SchemaManager.class.isAssignableFrom(callerClass) : "invalid calling class "
                + callerClass;
        return true;
    }

    // --------------- Stuff from SchemaMXBeanImpl

    @Override
    public Map<TableName, String> getUserTables() throws Exception {
        return getTables(true);
    }

    @Override
    public boolean isProtectedTable(int rowDefId) {
        return (rowDefId >= 100000 && rowDefId < 100100);
    }

    @Override
    public boolean isProtectedTable(String schema, String table) {
        return schema.equals("akiba_information_schema")
                || schema.equals("akiba_objects");
    }

    /**
     * Gets the DDLs for tables. Eventually we may want to distinguish between
     * user and group tables.
     * 
     * @param userTables
     *            whether to include user tables (as opposed to group tables)
     * @return a table -> ddl mapping
     * @throws Exception
     *             if there's any problem
     */
    private Map<TableName, String> getTables(boolean userTables)
            throws Exception {
        Map<TableName, String> ret = new TreeMap<TableName, String>();

        StringBuilder builder = new StringBuilder();

        for (CreateTableStruct table : getSchemaStructs()) {
            if (table.getSchemaName().equals("akiba_information_schema")) {
                continue;
            }
            if (table.getSchemaName().equals("akiba_objects") == userTables) {
                continue;
            }

            TableName tableName = new TableName(table.getSchemaName(),
                    table.getTableName());
            String createDdl = builder.append("CREATE TABLE ")
                    .append(table.getDdl()).toString();
            ret.put(tableName, createDdl);
            builder.setLength(0);
        }

        return ret;
    }

    @Override
    public SchemaId getSchemaID() throws Exception {
        return store.getPropertiesManager().getSchemaId();
    }

    @Override
    public void forceSchemaGenerationUpdate() throws Exception {
        store.getPropertiesManager().incrementSchemaId();
    }

    @Override
    public void createTable(String schemaName, String DDL) throws Exception {
        store.createTable(schemaName, DDL);
        store.getPropertiesManager().incrementSchemaId();
        acquireAIS();
    }

    @Override
    public void dropTable(String schema, String tableName) throws Exception {
        dropGroups(Arrays.asList(TableName.create(schema, tableName)));
    }

    /**
     * Drops all tables. If this succeeds, the schema generation will be
     * incremented.
     * 
     * @return the drop result
     * @throws Exception
     */
    @Override
    public void dropAllTables() throws Exception {
        try {
            final Collection<Integer> dropTables = getTablesToRefIds().values();
            store.dropTables(dropTables);
            store.getPropertiesManager().incrementSchemaId();
        } finally {
            acquireAIS();
        }
    }

    private void dropGroups(Collection<TableName> containingTables)
            throws Exception {
        for (TableName containingTable : containingTables) {
            if (containingTable.getSchemaName().equals("akiba_objects")
                    || containingTable.getSchemaName().equals(
                            "akiba_information_schema")) {
                throw new Exception("cannot drop tables in schema "
                        + containingTable.getSchemaName());
            }
        }

        Grouping grouping = GroupsBuilder.fromAis(getAisCopy(), null);
        final Set<String> groupsToDrop = new HashSet<String>();
        final Map<TableName, Integer> tablesToRefIds = getTablesToRefIds();
        for (TableName containingTable : containingTables) {
            if (grouping.containsTable(containingTable)) {
                groupsToDrop.add(grouping.getGroupFor(containingTable)
                        .getGroupName());
            }
        }
        if (groupsToDrop.isEmpty()) {
            return;
        }

        GroupingVisitor<List<Integer>> visitor = new GroupingVisitorStub<List<Integer>>() {
            private final List<Integer> dropTables = new ArrayList<Integer>();
            private boolean shouldDrop;

            @Override
            public void visitGroup(Group group, TableName rootTable) {
                shouldDrop = groupsToDrop.contains(group.getGroupName());
                if (shouldDrop) {
                    dropTable(rootTable);
                }
            }

            @Override
            public boolean startVisitingChildren() {
                return shouldDrop;
            }

            @Override
            public void visitChild(TableName parentName,
                    List<String> parentColumns, TableName childName,
                    List<String> childColumns) {
                assert shouldDrop : String.format("%s (%s) references %s (%s)",
                        childName, childColumns, parentName, parentColumns);
                dropTable(childName);
            }

            private void dropTable(TableName tableName) {
                dropTables.add(tablesToRefIds.get(tableName));
            }

            @Override
            public List<Integer> end() {
                return dropTables;
            }
        };

        List<Integer> dropTables = grouping.traverse(visitor);
        try {
            store.dropTables(dropTables);
            store.getPropertiesManager().incrementSchemaId();
        } finally {
            acquireAIS();
        }
    }

    private Map<TableName, Integer> getTablesToRefIds() {
        List<RowDef> rowDefs = store.getRowDefCache().getRowDefs();
        Map<TableName, Integer> ret = new HashMap<TableName, Integer>(
                rowDefs.size());
        for (RowDef rowDef : rowDefs) {
            TableName tableName = TableName.create(rowDef.getSchemaName(),
                    rowDef.getTableName());
            int id = rowDef.getRowDefId();
            Integer oldId = ret.put(tableName, id);
            assert oldId == null : "duplicate " + oldId + " for " + tableName;
        }
        return ret;
    }

    @Override
    public void dropSchema(String schemaName) throws Exception {
        store.dropSchema(schemaName);
    }

    public String getGrouping() throws Exception {
        AkibaInformationSchema ais = getAisCopy();
        String defaultSchema = null;

        for (UserTable userTable : ais.getUserTables().values()) {
            if (!userTable.getName().getSchemaName()
                    .equalsIgnoreCase("akiba_information_schema")) {
                defaultSchema = userTable.getName().getSchemaName();
                break;
            }
        }

        Grouping grouping = GroupsBuilder.fromAis(ais, defaultSchema);

        List<String> groupsToDrop = grouping
                .traverse(new GroupingVisitorStub<List<String>>() {
                    private final List<String> groups = new ArrayList<String>();

                    @Override
                    public void visitGroup(Group group, TableName rootTable) {
                        if (rootTable.getSchemaName().equalsIgnoreCase(
                                "akiba_information_schema")) {
                            groups.add(group.getGroupName());
                        }
                    }

                    @Override
                    public List<String> end() {
                        return groups;
                    }
                });

        GroupsBuilder builder = new GroupsBuilder(grouping);
        for (String groupToDrop : groupsToDrop) {
            builder.dropGroup(groupToDrop);
        }

        return grouping.toString();
    }

    @Override
    public List<String> getDDLs() throws Exception {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        writeAIS(printWriter);
        printWriter.flush();
        // StringWriter.flush() is a no-op
        stripSemicolons(stringWriter.getBuffer());
        String asString = stringWriter.toString();

        List<String> ret = new ArrayList<String>();
        ret.add("set default_storage_engine = akibandb");
        ret.add("create database if not exists `akiba_information_schema`");
        ret.add("use `akiba_information_schema`");

        final int schemaDdlsIndex = ret.size();
        ret.addAll(aisSchemaDdls);
        
        ListIterator<String> iter = ret.listIterator(schemaDdlsIndex);
        while (iter.hasNext()) {
            final String full = iter.next();
            assert full.charAt(full.length() - 1) == ';' : full;
            final String noSemi = full.substring(0, full.length() - 1);
            iter.set(noSemi.replaceAll("\n", ""));
        }
        ret.add("create schema if not exists `akiba_objects`");

        if (asString.length() > 0) {
            ret.addAll(Arrays.asList(stringWriter.toString().split("\n")));
        }
        return ret;
    }

    protected void writeAIS(PrintWriter writer) throws Exception {
        AkibaInformationSchema ais = getAisCopy();
        addGroupTables(writer, ais);
        addUserTables(writer);
    }

    private void addGroupTables(PrintWriter writer, AkibaInformationSchema ais) {
        ArrayList<String> ddls = new ArrayList<String>(
                new DDLGenerator().createAllGroupTables(ais));
        Collections.sort(ddls);

        for (String ddl : ddls) {
            writer.println(ddl);
        }
    }

    private void addUserTables(PrintWriter writer) throws Exception {
        Map<TableName, String> tables = getUserTables();

        String used = null;
        Set<String> createdSchemas = new HashSet<String>(tables.size());

        StringBuilder builder = new StringBuilder();

        for (Map.Entry<TableName, String> ddlEntry : tables.entrySet()) {
            String schema = ddlEntry.getKey().getSchemaName();
            String ddl = ddlEntry.getValue().trim();

            if (!schema.equals(used)) {
                if (createdSchemas.add(schema)) {
                    builder.append("create database if not exists ");
                    TableName.escape(schema, builder);
                    builder.append('\n');
                }
                builder.append("use ");
                TableName.escape(schema, builder);
                writer.println(builder.toString());
                builder.setLength(0);
                used = schema;
            }

            if (ddl.charAt(ddl.length() - 1) == ';') {
                ddl = ddl.substring(0, ddl.length() - 1);
            }

            writer.println(ddl);
        }
    }

    /**
     * Strips out any semicolon that's immediately followed by a \n.
     * 
     * @param buffer
     *            the io buffer
     */
    private void stripSemicolons(StringBuffer buffer) {
        for (int index = 0, len = buffer.length(); index < len; ++index) {
            if (buffer.charAt(index) == ';' && (index + 1) < len
                    && buffer.charAt(index + 1) == '\n') {
                buffer.deleteCharAt(index);
                --len;
                --index; // so that it stays the same after for's increment
                         // expression
            }
        }
    }

}
