package com.akiban.cserver.store;

import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_DEEP;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.cserver.CServerConfig;
import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.IndexDef;
import com.akiban.cserver.MySQLErrorConstants;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.RowType;
import com.akiban.cserver.TableStatistics;
import com.akiban.cserver.CServer.CreateTableStruct;
import com.akiban.cserver.message.ScanRowsRequest;
import com.akiban.util.Tap;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.TransactionRunnable;
import com.persistit.Transaction.CommitListener;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TransactionFailedException;
import com.persistit.logging.ApacheCommonsLogAdapter;

public class PersistitStore implements CServerConstants, MySQLErrorConstants,
        Store {

    final static int INITIAL_BUFFER_SIZE = 1024;

    private static final Log LOG = LogFactory.getLog(PersistitStore.class
            .getName());

    private static final String P_DATAPATH = "cserver.datapath";

    private static final String FIXED_ALLOCATION_PROPERTY_NAME = "cserver.fixed";

    private static final String PERSISTIT_PROPERTY_PREFIX = "persistit.";

    private static final String EXPERIMENTAL_SCHEMA_FLAG = "schema";

    // TODO -- this is a hack to enable setting/clearing the
    // deferIndex flag
    //
    private static final String AKIBAN_SPECIAL_SCHEMA_FLAG = "__akiban";

    private static final String AKIBAN_SPECIAL_DEFER_INDEXES_FLAG = "deferIndexes";

    private static final String AKIBAN_SPECIAL_FLUSH_INDEXES_FLAG = "flushIndexes";

    private static final String AKIBAN_SPECIAL_BUILD_INDEXES_FLAG = "buildIndexes";

    private static final String AKIBAN_SPECIAL_DELETE_INDEXES_FLAG = "deleteIndexes";

    private static final Tap WRITE_ROW_TAP = Tap.add(new Tap.PerThread(
            "write: write_row"));

    private static final Tap UPDATE_ROW_TAP = Tap.add(new Tap.PerThread(
            "write: update_row"));

    private static final Tap DELETE_ROW_TAP = Tap.add(new Tap.PerThread(
            "write: delete_row"));

    private static final Tap TX_COMMIT_TAP = Tap.add(new Tap.PerThread(
            "write: tx_commit"));

    private static final Tap TX_RETRY_TAP = Tap.add(new Tap.PerThread(
            "write: tx_retry", Tap.Count.class));

    private static final Tap NEW_COLLECTOR_TAP = Tap.add(new Tap.PerThread(
            "read: new_collector"));

    static final int MAX_TRANSACTION_RETRY_COUNT = 10;

    final static String SCHEMA_TREE_NAME = "_schema_";

    final static String BY_ID = "byId";

    final static String BY_NAME = "byName";

    final static String VOLUME_NAME = "akiban_data"; // TODO - select
    // database

    private final static int MEGA = 1024 * 1024;

    private final static int MAX_ROW_SIZE = 5000000;

    private final static int MAX_INDEX_TRANCHE_SIZE = 200 * MEGA;

    private final static int KEY_STATE_SIZE_OVERHEAD = 50;

    private final static long MEMORY_RESERVATION = 16 * MEGA;

    private final static float PERSISTIT_ALLOCATION_FRACTION = 0.5f;

    private final static Properties PERSISTIT_PROPERTIES = new Properties();

    static {
        PERSISTIT_PROPERTIES.put("logfile",
                "${datapath}/persistit_${timestamp}.log");
        PERSISTIT_PROPERTIES.put("buffer.count.8192", "1K");
        PERSISTIT_PROPERTIES.put("volume.1",
                "${datapath}/akiban_system.v0,create,pageSize:8K,initialSize:10K,e"
                        + "xtensionSize:1K,maximumSize:10G");
        PERSISTIT_PROPERTIES.put("volume.2",
                "${datapath}/akiban_txn.v0,create,pageSize:8K,initialSize:1M,e"
                        + "xtensionSize:1M,maximumSize:10G");
        PERSISTIT_PROPERTIES.put("volume.3", "${datapath}/" + VOLUME_NAME
                + ".v01,create,pageSize:8k,"
                + "initialSize:5M,extensionSize:5M,maximumSize:100G");
        PERSISTIT_PROPERTIES.put("sysvolume", "akiban_system");
        PERSISTIT_PROPERTIES.put("txnvolume", "akiban_txn");
        PERSISTIT_PROPERTIES.put("timeout", "60000");
        //
        // Temporary setup for testing Persistit 2.1
        //
        PERSISTIT_PROPERTIES.put("journalpath", "${datapath}/akiban_journal");
        PERSISTIT_PROPERTIES.put("logsize", "1G");
    }

    private static final String DEFAULT_DATAPATH = "/tmp/chunkserver_data";

    private boolean verbose = false;

    private final String experimental = "schema";

    private boolean coverEnabled = false;

    private boolean deferIndexes = false;

    private CServerConfig config;

    private Persistit db;

    private final RowDefCache rowDefCache;

    private PersistitStoreTableManager tableManager;

    private PersistitStoreIndexManager indexManager;

    private PersistitStoreSchemaManager schemaManager;

    private PersistitStorePropertiesManager propertiesManager;

    private boolean forceToDisk = false; // default to "group commit"

    private List<CommittedUpdateListener> updateListeners = Collections
            .synchronizedList(new ArrayList<CommittedUpdateListener>());

    // Using a Map<Thread, ...> instead of a ThreadLocal because we want to
    // clear all state in the shutDown() method.
    //
    private Map<Thread, Map<Integer, RowCollector>> sessionRowCollectorMap = new ConcurrentHashMap<Thread, Map<Integer, RowCollector>>();

    private final Map<String, SortedSet<KeyState>> deferredIndexKeys = new HashMap<String, SortedSet<KeyState>>();

    private int deferredIndexKeyLimit = MAX_INDEX_TRANCHE_SIZE;

    private final ThreadLocal<Map<String, List<Exchange>>> exchangeMapThreadLocal = new ThreadLocal<Map<String, List<Exchange>>>();

    public PersistitStore(final CServerConfig config, final RowDefCache cache)
            throws Exception {
        this.rowDefCache = cache;
        this.config = config;

    }

    public String getDataPath() {
        return config.property(P_DATAPATH, DEFAULT_DATAPATH);
    }

    private synchronized void createManagers() {
        this.tableManager = new PersistitStoreTableManager(this);
        this.indexManager = new PersistitStoreIndexManager(this);
        this.schemaManager = new PersistitStoreSchemaManager(this);
        this.propertiesManager = new PersistitStorePropertiesManager(this);
    }

    public synchronized void startUp() throws Exception {

        createManagers();
        // Util.printRuntimeInfo();

        if (db == null) {
            db = new Persistit();
            db.setPersistitLogger(new ApacheCommonsLogAdapter(LOG));
            //
            // This injects the "datapath" properties into the Persistit
            // properties; it is then referenced by substitution in other
            // Persistit properties.
            //
            final String path = getDataPath();
            db.setProperty("datapath", path);

            final boolean isUnitTest = "true".equals(config.property(
                    FIXED_ALLOCATION_PROPERTY_NAME, "false"));
            ensureDirectoryExists(path, false);

            if (!isUnitTest) {
                resetMemoryAllocation();
            }

            //
            // Override default property values with CServerConfig-specified
            // values.
            //
            for (final Map.Entry<Object, Object> entry : config.getProperties()
                    .entrySet()) {
                final String key = (String) entry.getKey();
                final String value = (String) entry.getValue();
                if (key.startsWith(PERSISTIT_PROPERTY_PREFIX)) {
                    PERSISTIT_PROPERTIES.setProperty(key
                            .substring(PERSISTIT_PROPERTY_PREFIX.length()),
                            value);
                }
            }

            db.initialize(PERSISTIT_PROPERTIES);

            if (LOG.isInfoEnabled()) {
                LOG.info("PersistitStore datapath="
                        + db.getProperty("datapath") + " 8k_buffers="
                        + db.getProperty("buffer.count.8192"));
            }
            db.getManagement().setDisplayFilter(
                    new RowDataDisplayFilter(this, db.getManagement()
                            .getDisplayFilter()));

            tableManager.startUp();
            propertiesManager.startUp();

            // TODO - temporary for testing

            coverEnabled = "true".equalsIgnoreCase(config.property(
                    "cserver.cover", "true"));
        }
    }

    private static void ensureDirectoryExists(String path,
            boolean alreadyTriedCreatingDirectory) throws StoreException {
        File dir = new File(path);
        if (dir.exists()) {
            if (!dir.isDirectory()) {
                throw new StoreException(-1, String.format(
                        "%s exists but is not a directory", dir));
            }
        } else {
            if (alreadyTriedCreatingDirectory) {
                throw new StoreException(-1, String.format(
                        "Unable to create directory %s. Permissions problem?",
                        dir));
            } else {
                dir.mkdirs();
                ensureDirectoryExists(path, true);
            }
        }
    }

    private void resetMemoryAllocation() {
        final long allocation = (long) ((CServerUtil.availableMemory() - MEMORY_RESERVATION) * PERSISTIT_ALLOCATION_FRACTION);
        final long buffers8k = Math.max(allocation / (8192 + 4096), 512);
        PERSISTIT_PROPERTIES.setProperty("buffer.count.8192", Long
                .toString(buffers8k));
    }

    private synchronized void destroyManagers() throws Exception {
        tableManager.shutDown();
        tableManager = null;
        indexManager.shutDown();
        indexManager = null;
    }

    public synchronized void shutDown() throws Exception {
        if (db != null) {
            destroyManagers();
            db.shutdownGUI();
            db.close();
            db = null;
            sessionRowCollectorMap.clear();
        }
    }

    public Persistit getDb() {
        return db;
    }

    public Exchange getExchange(final RowDef rowDef, final IndexDef indexDef)
            throws PersistitException {
        final String treeName;
        if (indexDef == null) {
            final RowDef groupRowDef = rowDef.isGroupTable() ? rowDef
                    : rowDefCache.getRowDef(rowDef.getGroupRowDefId());
            treeName = groupRowDef.getTreeName();
        } else {
            treeName = indexDef.getTreeName();
        }
        return getExchange(treeName);
    }

    public void releaseExchange(final Exchange exchange) {
        if (exchange != null) {
            Map<String, List<Exchange>> exchangeMap = getExchangeMap();
            String treeName = exchange.getTree().getName();
            List<Exchange> list = exchangeMap.get(treeName);
            if (list == null) {
                list = new ArrayList<Exchange>();
                exchangeMap.put(treeName, list);
            }
            list.add(exchange);
        }
    }

    public Exchange getExchange(final String treeName)
            throws PersistitException {
        Exchange ex;
        Map<String, List<Exchange>> exchangeMap = getExchangeMap();
        List<Exchange> list = exchangeMap.get(treeName);
        if (list == null || list.isEmpty()) {
            ex = db.getExchange(VOLUME_NAME, treeName, true).clear();
        } else {
            ex = list.remove(list.size() - 1);
        }
        return ex;
    }

    private Map<String, List<Exchange>> getExchangeMap() {
        Map<String, List<Exchange>> exchangeMap = exchangeMapThreadLocal.get();
        if (exchangeMap == null) {
            exchangeMap = new HashMap<String, List<Exchange>>();
            exchangeMapThreadLocal.set(exchangeMap);
        }
        return exchangeMap;
    }

    /**
     * Given a RowData for a table, construct an hkey for a row in the table.
     * For a non-root table, this method uses the parent join columns as needed
     * to find the hkey of the parent table. The attempt to look up the parent
     * row may result in a StoreException due to a missing parent row; this is
     * expressed as a HA_ERR_NO_REFERENCED_ROW error.
     *
     * @param hEx
     * @param rowData
     * @throws StoreException
     * @throws PersistitException
     */
    void constructHKey(final Exchange hEx, final RowDef rowDef,
            final RowData rowData) throws PersistitException, StoreException {
        final Key hkey = hEx.getKey();
        hkey.clear();
        switch (rowDef.getRowType()) {
        case GROUP:
            throw new UnsupportedOperationException(
                    "Cannot insert into a group table: "
                            + rowDef.getTableName());
        case ROOT:
            hkey.append(rowDef.getOrdinal());
            appendKeyFields(hkey, rowDef, rowData, rowDef.getPkFields());
            break;

        case CHILD: {
            final RowDef parentRowDef = rowDefCache.getRowDef(rowDef
                    .getParentRowDefId());
            hkey.append(parentRowDef.getOrdinal());
            appendKeyFields(hkey, rowDef, rowData, rowDef.getParentJoinFields());
            if (!hEx.isValueDefined()) {
                throw new StoreException(HA_ERR_NO_REFERENCED_ROW, hkey
                        .toString());
            }
            hkey.append(rowDef.getOrdinal());
            appendKeyFields(hkey, rowDef, rowData, rowDef.getPkFields());
            break;
        }

        case GRANDCHILD: {
            final RowDef parentRowDef = rowDefCache.getRowDef(rowDef
                    .getParentRowDefId());
            final Exchange iEx = getExchange(rowDef, parentRowDef
                    .getPKIndexDef());
            constructParentPKIndexKey(iEx.getKey(), rowDef, rowData);
            if (!iEx.hasChildren()) {
                throw new StoreException(HA_ERR_NO_REFERENCED_ROW, iEx.getKey()
                        .toString());
            }
            boolean next = iEx.next(true);
            assert next;
            constructHKeyFromIndexKey(hkey, iEx.getKey(), parentRowDef
                    .getPKIndexDef());
            hkey.append(rowDef.getOrdinal());
            appendKeyFields(hkey, rowDef, rowData, rowDef.getPkFields());
            releaseExchange(iEx);
        }
        }
    }

    void constructHKey(final Exchange hEx, final RowDef rowDef,
            final int[] ordinals, final int[] nKeyColumns,
            final FieldDef[] fieldDefs, final Object[] hKeyValues)
            throws Exception {
        final Key hkey = hEx.getKey();
        hkey.clear();
        int k = 0;
        for (int i = 0; i < ordinals.length; i++) {
            hkey.append(ordinals[i]);
            for (int j = 0; j < nKeyColumns[i]; j++) {
                appendKeyField(hkey, fieldDefs[k], hKeyValues[k]);
                k++;
            }
        }
    }

    /**
     * Given a RowData, the hkey where it will be stored, and an IndexDef for a
     * table, construct the index key.
     *
     * @param rowData
     * @param indexDef
     */
    void constructIndexKey(final Key iKey, final RowData rowData,
            final IndexDef indexDef, final Key hKey) throws PersistitException {
        final IndexDef.H2I[] fassoc = indexDef.getIndexKeyFields();
        iKey.clear();
        for (int index = 0; index < fassoc.length; index++) {
            final IndexDef.H2I assoc = fassoc[index];
            if (assoc.getFieldIndex() >= 0) {
                final int fieldIndex = assoc.getFieldIndex();
                final RowDef rowDef = indexDef.getRowDef();
                appendKeyField(iKey, rowDef.getFieldDef(fieldIndex), rowData);
            } else if (assoc.getHkeyLoc() >= 0) {
                appendKeyFieldFromKey(hKey, iKey, assoc.getHkeyLoc());
            } else {
                throw new IllegalStateException("Invalid FA");
            }
        }
    }

    /**
     * Given an index key and an indexDef, construct the corresponding hkey for
     * the row identified by the index key.
     *
     * @param hKey
     * @param indexKey
     * @param indexDef
     */
    void constructHKeyFromIndexKey(final Key hKey, final Key indexKey,
            final IndexDef indexDef) {
        final IndexDef.I2H[] fassoc = indexDef.getHkeyFields();
        hKey.clear();
        for (int index = 0; index < fassoc.length; index++) {
            final IndexDef.I2H fa = fassoc[index];
            if (fa.isOrdinalType()) {
                hKey.append(fa.getOrdinal());
            } else {
                final int depth = fassoc[index].getIndexKeyLoc();
                if (depth < 0 || depth > indexKey.getDepth()) {
                    throw new IllegalStateException(
                            "IndexKey too shallow - requires depth=" + depth
                                    + ": " + indexKey);
                }
                appendKeyFieldFromKey(indexKey, hKey, fa.getIndexKeyLoc());
            }
        }
    }

    /**
     * Given a RowData for a table, construct an Exchange set up with a Key that
     * is the prefix of the parent's primary key index key.
     *
     * @param rowData
     */
    void constructParentPKIndexKey(final Key iKey, final RowDef rowDef,
            final RowData rowData) {
        iKey.clear();
        appendKeyFields(iKey, rowDef, rowData, rowDef.getParentJoinFields());
    }

    void appendKeyFields(final Key key, final RowDef rowDef,
            final RowData rowData, final int[] fields) {
        for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
            final FieldDef fieldDef = rowDef.getFieldDef(fields[fieldIndex]);
            appendKeyField(key, fieldDef, rowData);
        }
    }

    void appendKeyField(final Key key, final FieldDef fieldDef,
            final RowData rowData) {
        fieldDef.getEncoding().toKey(fieldDef, rowData, key);
    }

    private void appendKeyFieldFromKey(final Key fromKey, final Key toKey,
            final int depth) {
        fromKey.indexTo(depth);
        int from = fromKey.getIndex();
        fromKey.indexTo(depth + 1);
        int to = fromKey.getIndex();
        if (from >= 0 && to >= 0 && to > from) {
            System.arraycopy(fromKey.getEncodedBytes(), from, toKey
                    .getEncodedBytes(), toKey.getEncodedSize(), to - from);
            toKey.setEncodedSize(toKey.getEncodedSize() + to - from);
        }

    }

    private void appendKeyField(final Key key, final FieldDef fieldDef,
            Object value) {
        fieldDef.getEncoding().toKey(fieldDef, value, key);
    }

    // --------------------- Implement Store interface --------------------

    @Override
    public RowDefCache getRowDefCache() {
        return rowDefCache;
    }

    private static <T> T errorIfNull(String description, T object) {
        if (object == null) {
            throw new NullPointerException(description + " is null; did you call startUp()?");
        }
        return object;
    }

    public PersistitStoreTableManager getTableManager() {
        return errorIfNull("table manager", tableManager);
    }

    public PersistitStoreIndexManager getIndexManager() {
        return errorIfNull("index manager", indexManager);
    }

    public PersistitStorePropertiesManager getPropertiesManager() {
        return errorIfNull("properties manager", propertiesManager);
    }

    public void setOrdinals() throws Exception {
        for (final RowDef groupRowDef : rowDefCache.getRowDefs()) {
            if (groupRowDef.isGroupTable()) {
                // groupTable has no ordinal
                groupRowDef.setOrdinal(0);
                final HashSet<Integer> assigned = new HashSet<Integer>();
                //
                // First pass: merge already assigned values
                //
                for (final RowDef userRowDef : groupRowDef
                        .getUserTableRowDefs()) {
                    final TableStatus tableStatus = tableManager
                            .getTableStatus(userRowDef.getRowDefId());
                    if (tableStatus.getOrdinal() != 0
                            && userRowDef.getOrdinal() != 0
                            && tableStatus.getOrdinal() != userRowDef
                                    .getOrdinal()) {
                        throw new IllegalStateException("Mismatched ordinals: "
                                + userRowDef.getOrdinal() + "/"
                                + tableStatus.getOrdinal());
                    }
                    int ordinal = 0;
                    if (tableStatus.getOrdinal() != 0) {
                        ordinal = tableStatus.getOrdinal();
                        userRowDef.setOrdinal(ordinal);
                    } else if (userRowDef.getOrdinal() != 0
                            && tableStatus.getOrdinal() == 0) {
                        ordinal = userRowDef.getOrdinal();
                        tableStatus.setOrdinal(ordinal);
                    }
                    if (ordinal != 0 && !assigned.add(ordinal)) {
                        throw new IllegalStateException(
                                "Non-unique ordinal value " + ordinal
                                        + " added to " + assigned);
                    }
                }
                int nextOrdinal = 1;
                for (final RowDef userRowDef : groupRowDef
                        .getUserTableRowDefs()) {
                    final TableStatus tableStatus = tableManager
                            .getTableStatus(userRowDef.getRowDefId());
                    if (userRowDef.getOrdinal() == 0) {
                        // find an unassigned value. Here we could try to
                        // optimize layout
                        // by assigning "bushy" values in some optimal pattern
                        // (if we knew that was...)
                        for (; assigned.contains(nextOrdinal); nextOrdinal++) {
                        }
                        tableStatus.setOrdinal(nextOrdinal);
                        userRowDef.setOrdinal(nextOrdinal);
                        assigned.add(nextOrdinal);
                    }
                }
                if (assigned.size() != groupRowDef.getUserTableRowDefs().length) {
                    throw new IllegalStateException("Inconsistent ordinal "
                            + "number assignments: " + assigned);
                }
            }
        }
    }

    @Override
    public void setVerbose(final boolean verbose) {
        this.verbose = verbose;
    }

    @Override
    public boolean isVerbose() {
        return verbose;
    }

    public boolean isExperimentalSchema() {
        return experimental.contains(EXPERIMENTAL_SCHEMA_FLAG);
    }

    public boolean isCoveringIndexSupportEnabled() {
        return coverEnabled;
    }

    @Override
    public int createTable(final String schemaName, final String ddl) {
        if (AKIBAN_SPECIAL_SCHEMA_FLAG.equals(schemaName)) {
            deferIndexes = ddl.contains(AKIBAN_SPECIAL_DEFER_INDEXES_FLAG);
            if (ddl.contains(AKIBAN_SPECIAL_FLUSH_INDEXES_FLAG)) {
                flushIndexes();
            }
            if (ddl.contains(AKIBAN_SPECIAL_DELETE_INDEXES_FLAG)) {
                deleteIndexes(ddl);
            }
            if (ddl.contains(AKIBAN_SPECIAL_BUILD_INDEXES_FLAG)) {
                buildIndexes(ddl);
            }
            return OK;
        }
        Transaction transaction = db.getTransaction();
        try {
            final AtomicReference<Integer> tableId = new AtomicReference<Integer>(null);
            transaction.run(new TransactionRunnable() {
                @Override
                public void runTransaction() {
                    final int result = schemaManager.createTable(schemaName, ddl, tableId, rowDefCache);
                    if (result != OK) {
                        throw new RollbackException("error " + result);
                    }
                }
            });
            Integer tableIdInteger = tableId.get();
            if (tableIdInteger != null) {
                tableManager.getTableStatus(tableIdInteger).reset();
            }
        } catch (Exception e) {
            LOG.error("Failure while installing " + ddl, e);
            return ERR;
        }
        return OK;
    }

    public List<CreateTableStruct> getSchema() throws Exception {
        Transaction transaction = db.getTransaction();
        final List<CreateTableStruct> result = new ArrayList<CreateTableStruct>();
        transaction.run(new TransactionRunnable() {

            @Override
            public void runTransaction() throws PersistitException,
                    RollbackException {
                result.clear();
                schemaManager.populateSchema(result);
            }
        });
        return result;
    }

    @Override
    public int writeRow(final RowData rowData) {
        final int rowDefId = rowData.getRowDefId();

        if (rowData.getRowSize() > MAX_ROW_SIZE) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("RowData size " + rowData.getRowSize()
                        + " is larger than current limit of " + MAX_ROW_SIZE
                        + " bytes");
            }
        }

        WRITE_ROW_TAP.in();
        final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
        Transaction transaction = null;
        Exchange hEx = null;
        try {
            hEx = getExchange(rowDef, null);
            transaction = db.getTransaction();
            int retries = MAX_TRANSACTION_RETRY_COUNT;
            for (;;) {
                transaction.begin();
                try {
                    final TableStatus ts = tableManager
                            .getTableStatus(rowDefId);
                    if (ts.isDeleted()) {
                        throw new StoreException(HA_ERR_NO_SUCH_TABLE,
                                "Deleted table " + rowDefId);
                    }

                    //
                    // Does the heavy lifting of looking up the full hkey in
                    // parent's primary index if necessary.
                    //
                    constructHKey(hEx, rowDef, rowData);

                    if (hEx.isValueDefined()) {
                        throw new StoreException(HA_ERR_FOUND_DUPP_KEY,
                                "Non-unique key " + hEx.getKey());
                    }

                    final int start = rowData.getInnerStart();
                    final int size = rowData.getInnerSize();
                    hEx.getValue().ensureFit(size);
                    System.arraycopy(rowData.getBytes(), start, hEx.getValue()
                            .getEncodedBytes(), 0, size);
                    hEx.getValue().setEncodedSize(size);

                    // Store the h-row
                    hEx.store();
                    if (rowDef.getAutoIncrementField() >= 0) {
                        final long location = rowDef.fieldLocation(rowData,
                                rowDef.getAutoIncrementField());
                        if (location != 0) {
                            final long autoIncrementValue = rowData
                                    .getIntegerValue((int) location,
                                            (int) (location >>> 32));
                            if (autoIncrementValue > ts.getAutoIncrementValue()) {
                                ts.setAutoIncrementValue(autoIncrementValue);
                            }
                        }
                    }
                    ts.incrementRowCount(1);
                    ts.written();

                    for (final IndexDef indexDef : rowDef.getIndexDefs()) {
                        //
                        // Insert the index keys (except for the case of a
                        // root table's PK index.)
                        //
                        if (!indexDef.isHKeyEquivalent())
                            insertIntoIndex(indexDef, rowData, hEx.getKey(),
                                    deferIndexes);
                    }

                    TX_COMMIT_TAP.in();
                    if (updateListeners.isEmpty()) {
                        transaction.commit(forceToDisk);
                    } else {
                        final KeyState keyState = new KeyState(hEx.getKey());
                        transaction.commit(new CommitListener() {
                            public void committed() {
                                for (final CommittedUpdateListener cul : updateListeners) {
                                    cul.inserted(keyState, rowDef, rowData);
                                }
                            }
                        }, forceToDisk);
                    }
                    break;
                } catch (RollbackException re) {
                    TX_RETRY_TAP.out();
                    if (--retries < 0) {
                        throw new TransactionFailedException();
                    }
                } finally {
                    TX_COMMIT_TAP.out();
                    transaction.end();
                }
            }
            if (deferredIndexKeyLimit <= 0) {
                putAllDeferredIndexKeys();
            }
            return OK;
        } catch (StoreException e) {
            if (verbose && LOG.isInfoEnabled()) {
                LOG.info("writeRow error " + e.getResult());
            }
            return e.getResult();
        } catch (Throwable t) {
            LOG.error("writeRow failed", t);
            return ERR;
        } finally {
            releaseExchange(hEx);
            WRITE_ROW_TAP.out();
        }
    }

    @Override
    public int writeRowForBulkLoad(final Exchange hEx, final RowDef rowDef,
            final RowData rowData, final int[] ordinals,
            final int[] nKeyColumns, final FieldDef[] fieldDefs,
            final Object[] hKeyValues) throws Exception {
/*
        if (verbose && LOG.isInfoEnabled()) {
            LOG.info("BulkLoad writeRow: " + rowData.toString(rowDefCache));
        }
*/

        try {
            constructHKey(hEx, rowDef, ordinals, nKeyColumns, fieldDefs, hKeyValues);
            final int start = rowData.getInnerStart();
            final int size = rowData.getInnerSize();
            hEx.getValue().ensureFit(size);
            System.arraycopy(rowData.getBytes(), start, hEx.getValue().getEncodedBytes(), 0, size);
            hEx.getValue().setEncodedSize(size);
            // Store the h-row
            hEx.store();
/*
            for (final IndexDef indexDef : rowDef.getIndexDefs()) {
                // Insert the index keys (except for the case of a
                // root table's PK index.)
                if (!indexDef.isHKeyEquivalent()) {
                    insertIntoIndex(indexDef, rowData, hEx.getKey(), deferIndexes);
                }
            }
            if (deferredIndexKeyLimit <= 0) {
                putAllDeferredIndexKeys();
            }
*/
            return OK;
        } catch (StoreException e) {
            LOG.warn("Caught exception while writing row " + rowData.toString(rowDefCache) + ": ", e);
            return e.getResult();
        } catch (Throwable t) {
            LOG.warn("Caught exception while writing row " + rowData.toString(rowDefCache) + ": ", t);
            t.printStackTrace();
            return ERR;
        }
    }

    @Override
    public void updateTableStats(RowDef rowDef, long rowCount)
            throws PersistitException, StoreException {
        int rowDefId = rowDef.getRowDefId();
        TableStatus tableStatus = tableManager.getTableStatus(rowDefId);
        if (tableStatus.isDeleted()) {
            throw new StoreException(HA_ERR_NO_SUCH_TABLE, String.format(
                    "Deleted table %s", rowDefId));
        }
        tableStatus.setRowCount(rowCount);
        tableStatus.updated();
        tableManager.saveStatus(tableStatus);
    }

    @Override
    public int deleteRow(final RowData rowData) throws Exception {
        final int rowDefId = rowData.getRowDefId();

        final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
        Transaction transaction = null;
        Exchange hEx = null;
        ;
        try {
            hEx = getExchange(rowDef, null);

            transaction = db.getTransaction();
            int retries = MAX_TRANSACTION_RETRY_COUNT;
            for (;;) {
                transaction.begin();
                try {
                    final TableStatus ts = tableManager
                            .getTableStatus(rowDefId);
                    if (ts.isDeleted()) {
                        throw new StoreException(HA_ERR_NO_SUCH_TABLE,
                                "Deleted table " + rowDefId);
                    }

                    constructHKey(hEx, rowDef, rowData);
                    hEx.fetch();
                    //
                    // Verify that the row exists
                    //
                    if (!hEx.getValue().isDefined()) {
                        throw new StoreException(HA_ERR_RECORD_DELETED,
                                "Missing record at key " + hEx.getKey());
                    }
                    //
                    // Verify that the row hasn't changed. Note: at some point
                    // we may want to optimize the protocol to send only PK and
                    // FK fields in oldRowData, in which case this test will
                    // need to change.
                    //
                    // TODO - review. With covering indexes, that day has come.
                    // We can no longer do this comparison when the "old" row
                    // has only its PK fields.
                    //
                    // final int oldStart = rowData.getInnerStart();
                    // final int oldSize = rowData.getInnerSize();
                    // if (!bytesEqual(rowData.getBytes(), oldStart, oldSize,
                    // hEx
                    // .getValue().getEncodedBytes(), 0, hEx.getValue()
                    // .getEncodedSize())) {
                    // throw new StoreException(HA_ERR_RECORD_CHANGED,
                    // "Record changed at key " + hEx.getKey());
                    // }

                    //
                    // For Iteration 9 we disallow deleting rows that would
                    // cascade to child rows.
                    //
                    if (hEx.hasChildren()) {
                        throw new StoreException(HA_ERR_ROW_IS_REFERENCED,
                                "Can't cascade DELETE: " + hEx.getKey());

                    }

                    // Remove the h-row
                    hEx.remove();
                    ts.incrementRowCount(-1);
                    ts.deleted();
                    // Remove the indexes, including the PK index
                    for (final IndexDef indexDef : rowDef.getIndexDefs()) {
                        if (!indexDef.isHKeyEquivalent()) {
                            deleteIndex(indexDef, rowDef, rowData, hEx.getKey());
                        }
                    }
                    if (updateListeners.isEmpty()) {
                        transaction.commit(forceToDisk);
                    } else {
                        final KeyState keyState = new KeyState(hEx.getKey());
                        transaction.commit(new CommitListener() {
                            public void committed() {
                                for (final CommittedUpdateListener cul : updateListeners) {
                                    cul.deleted(keyState, rowDef, rowData);
                                }
                            }
                        }, forceToDisk);
                    }
                    return OK;
                } catch (RollbackException re) {
                    if (--retries < 0) {
                        throw new TransactionFailedException();
                    }
                } finally {
                    transaction.end();
                }
            }
        } catch (StoreException e) {
            if (verbose && LOG.isInfoEnabled()) {
                LOG.info("deleteRow error " + e.getResult(), e);
            }
            return e.getResult();
        } catch (Throwable t) {
            LOG.error("deleteRow failed: ", t);
            return ERR;
        } finally {
            releaseExchange(hEx);
        }
    }

    @Override
    public int updateRow(final RowData oldRowData, final RowData newRowData) {
        final int rowDefId = oldRowData.getRowDefId();

        if (newRowData.getRowDefId() != rowDefId) {
            throw new IllegalArgumentException(
                    "RowData values have different rowDefId values: ("
                            + rowDefId + "," + newRowData.getRowDefId() + ")");
        }
        final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
        Transaction transaction = null;
        Exchange hEx = null;
        try {
            hEx = getExchange(rowDef, null);
            transaction = db.getTransaction();
            int retries = MAX_TRANSACTION_RETRY_COUNT;
            for (;;) {
                transaction.begin();
                try {
                    final TableStatus ts = tableManager
                            .getTableStatus(rowDefId);
                    if (ts.isDeleted()) {
                        throw new StoreException(HA_ERR_NO_SUCH_TABLE,
                                "Deleted table " + rowDefId);
                    }
                    constructHKey(hEx, rowDef, oldRowData);
                    hEx.fetch();
                    //
                    // Verify that the row exists
                    //
                    if (!hEx.getValue().isDefined()) {
                        throw new StoreException(HA_ERR_RECORD_DELETED,
                                "Missing record at key " + hEx.getKey());
                    }

                    int status = OK;
                    //
                    // Verify that it hasn't changed. Note: at some point we
                    // may want to optimize the protocol to send only PK and FK
                    // fields in oldRowData, in which case this test will need
                    // to change.
                    //
                    //
                    // For Iteration 11, this logic is disabled because the
                    // SELECT for update may have used a covering index, in
                    // which case the oldRowData will be incomplete.
                    //
                    // final int oldStart = oldRowData.getInnerStart();
                    // final int oldSize = oldRowData.getInnerSize();
                    // if (!bytesEqual(oldRowData.getBytes(), oldStart, oldSize,
                    // hEx.getValue().getEncodedBytes(), 0, hEx.getValue()
                    // .getEncodedSize())) {
                    // throw new StoreException(HA_ERR_RECORD_CHANGED,
                    // "Record changed at key " + hEx.getKey());
                    // }
                    //
                    // For Iteration 9, verify that only non-PK/FK fields are
                    // changing - i.e., that the hkey will be the same.
                    //
                    if (!fieldsEqual(rowDef, oldRowData, newRowData, rowDef
                            .getPKIndexDef().getFields())) {
                        if (hEx.hasChildren()) {
                            throw new StoreException(HA_ERR_ROW_IS_REFERENCED,
                                    "Can't cascade UPDATE on PK field: "
                                            + hEx.getKey());
                        }
                        status = deleteRow(oldRowData);
                        if (status == OK) {
                            status = writeRow(newRowData);
                        }
                    } else {
                        final int start = newRowData.getInnerStart();
                        final int size = newRowData.getInnerSize();
                        hEx.getValue().ensureFit(size);
                        System.arraycopy(newRowData.getBytes(), start, hEx
                                .getValue().getEncodedBytes(), 0, size);
                        hEx.getValue().setEncodedSize(size);

                        // Store the h-row
                        hEx.store();
                        ts.updated();

                        // Update the indexes
                        //
                        for (final IndexDef indexDef : rowDef.getIndexDefs()) {
                            if (!indexDef.isHKeyEquivalent()) {
                                updateIndex(indexDef, rowDef, oldRowData,
                                        newRowData, hEx.getKey());
                            }
                        }
                    }
                    if (updateListeners.isEmpty()) {
                        transaction.commit(forceToDisk);
                    } else {
                        final KeyState keyState = new KeyState(hEx.getKey());
                        transaction.commit(new CommitListener() {
                            public void committed() {
                                for (final CommittedUpdateListener cul : updateListeners) {
                                    cul.updated(keyState, rowDef, oldRowData,
                                            newRowData);
                                }
                            }
                        }, forceToDisk);
                    }
                    return status;
                } catch (RollbackException re) {
                    if (--retries < 0) {
                        throw new TransactionFailedException();
                    }
                } finally {
                    transaction.end();
                }
            }
        } catch (StoreException e) {
            if (verbose && LOG.isInfoEnabled()) {
                LOG.info("updateRow error " + e.getResult(), e);
            }
            return e.getResult();
        } catch (Throwable t) {
            LOG.error("updateRow failed: ", t);
            return ERR;
        } finally {
            releaseExchange(hEx);
        }
    }

    /**
     * Remove contents of table. TODO: remove user table data from within a
     * group.
     */
    @Override
    public int truncateTable(final int rowDefId) throws Exception {

        final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
        Transaction transaction = null;

        RowDef groupRowDef = rowDef.isGroupTable() ? rowDef : rowDefCache
                .getRowDef(rowDef.getGroupRowDefId());
        try {
            transaction = db.getTransaction();
            int retries = MAX_TRANSACTION_RETRY_COUNT;
            for (;;) {

                transaction.begin();

                try {
                    final TableStatus ts = tableManager
                            .getTableStatus(rowDefId);
                    if (ts.isDeleted()) {
                        throw new StoreException(HA_ERR_NO_SUCH_TABLE,
                                "Deleted table " + rowDefId);
                    }
                    //
                    // Remove the index trees
                    //
                    for (final IndexDef indexDef : groupRowDef.getIndexDefs()) {
                        if (!indexDef.isHKeyEquivalent()) {
                            final Exchange iEx = getExchange(groupRowDef,
                                    indexDef);
                            iEx.removeAll();
                            releaseExchange(iEx);
                        }
                        indexManager.deleteIndexAnalysis(indexDef);
                    }
                    for (final RowDef userRowDef : groupRowDef
                            .getUserTableRowDefs()) {
                        for (final IndexDef indexDef : userRowDef
                                .getIndexDefs()) {
                            indexManager.deleteIndexAnalysis(indexDef);
                        }
                    }
                    //
                    // remove the htable tree
                    //
                    final Exchange hEx = getExchange(groupRowDef, null);
                    hEx.removeAll();
                    releaseExchange(hEx);
                    for (int i = 0; i < groupRowDef.getUserTableRowDefs().length; i++) {
                        final int childId = groupRowDef.getUserTableRowDefs()[i]
                                .getRowDefId();
                        final TableStatus ts1 = tableManager
                                .getTableStatus(childId);
                        ts1.setRowCount(Long.MIN_VALUE);
                        ts1.deleted();
                    }
                    final TableStatus ts0 = tableManager
                            .getTableStatus(groupRowDef.getRowDefId());
                    ts0.setRowCount(0);
                    ts0.deleted();

                    transaction.commit(forceToDisk);
                    return OK;

                } catch (RollbackException re) {
                    if (--retries < 0) {
                        throw new TransactionFailedException();
                    }
                } finally {
                    transaction.end();
                }
            }
        } catch (StoreException e) {
            if (/** verbose && **/
            LOG.isInfoEnabled()) {
                LOG.info("truncateTable error " + e.getResult(), e);
            }
            return e.getResult();
        } catch (Throwable t) {
            LOG.error("trucateTable failed: ", t);
            return ERR;
        }
    }

    @Override
    public int dropTable(int rowDefId) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        list.add(rowDefId);
        return dropTables(list);
    }

    /**
     * Remove contents of table and its schema information. TODO: remove user
     * table data from within a group.
     * clients.
     */
    @Override
    public int dropTables(final Collection<Integer> rowDefIds) throws Exception {
        List<Integer> myRowDefIds = new ArrayList(rowDefIds);
        // Never drop group tables
        for (Iterator<Integer> iter = myRowDefIds.iterator();
                iter.hasNext(); ) {
            int rowDefId = iter.next();
            RowDef rowDef = rowDefCache.getRowDef(rowDefId);
            if (rowDef.isGroupTable()) {
                iter.remove();
            }
        }
        if (myRowDefIds.isEmpty()) {
            return OK;
        }

        try {
            final Transaction transaction = db.getTransaction();
            int retries = MAX_TRANSACTION_RETRY_COUNT;
            final List<String> tablesToForget = new ArrayList<String>();
            for (;;) {
                transaction.begin();
                try {
                    tablesToForget.clear();
                    for (final int rowDefId : myRowDefIds) {
                        final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
                        final int status = schemaManager.dropCreateTable(rowDef.getSchemaName(), rowDef.getTableName());
                        if (status != OK) {
                            return status;
                        }
                        tablesToForget.add(rowDef.getTableName());
                        final TableStatus ts = tableManager
                                .getTableStatus(rowDefId);
                        // if (ts.isDeleted()) {
                        // throw new StoreException(HA_ERR_NO_SUCH_TABLE, "Table "
                        // + rowDef.getTableName() + " has been deleted");
                        // }
                        boolean deleteGroup = true;
                        ts.setDeleted(true);
                        tableManager.saveStatus(ts);
                        final RowDef groupRowDef;
                        if (rowDef.isGroupTable()) {
                            groupRowDef = rowDef;
                        } else {
                            groupRowDef = getRowDefCache().getRowDef(
                                    rowDef.getGroupRowDefId());
                            for (int i = 0; deleteGroup
                                    && i < groupRowDef.getUserTableRowDefs().length; i++) {
                                final int childId = groupRowDef
                                        .getUserTableRowDefs()[i].getRowDefId();
                                final TableStatus childStatus = tableManager
                                        .getTableStatus(childId);
                                deleteGroup &= childStatus.isDeleted();
                            }
                        }
                        if (deleteGroup) {
                            final int gstatus = schemaManager.dropCreateTable(
                                    groupRowDef.getSchemaName(), groupRowDef
                                            .getTableName());
                            if (gstatus != OK) {
                                return gstatus;
                            }
                            tableManager.getTableStatus(groupRowDef.getRowDefId())
                                    .setDeleted(true);
                            // IOException
                            // Remove the index trees
                            //
                            for (final IndexDef indexDef : groupRowDef
                                    .getIndexDefs()) {
                                if (!indexDef.isHKeyEquivalent()) {
                                    final Exchange iEx = getExchange(groupRowDef,
                                            indexDef);
                                    iEx.removeAll();
                                    releaseExchange(iEx);
                                }
                                indexManager.deleteIndexAnalysis(indexDef);
                            }
                            for (final RowDef userRowDef : groupRowDef
                                    .getUserTableRowDefs()) {
                                for (final IndexDef indexDef : userRowDef
                                        .getIndexDefs()) {
                                    indexManager.deleteIndexAnalysis(indexDef);
                                }
                            }
                            //
                            // remove the htable tree
                            //
                            final Exchange hEx = getExchange(groupRowDef, null);
                            hEx.removeAll();
                            releaseExchange(hEx);

                            for (int i = 0; i < groupRowDef.getUserTableRowDefs().length; i++) {
                                final int childId = groupRowDef
                                        .getUserTableRowDefs()[i].getRowDefId();
                                tableManager.deleteStatus(childId);
                            }
                            tableManager.deleteStatus(groupRowDef.getRowDefId());
                        }
                    }
                    
                    transaction.commit(forceToDisk);
                    for (String tableToForget : tablesToForget) {
                        schemaManager.forgetTableColumns(tableToForget);
                    }
                    return OK;
                }
                catch (RollbackException e) {
                    if (--retries < 0) {
                        throw new TransactionFailedException();
                    }
                } finally {
                    transaction.end();
                }
            }
            // } catch (StoreException e) {
            // if (verbose &&LOG.isInfoEnabled()) {
            // LOG.info("dropTable error " + e.getResult(), e);
            // }
            // return e.getResult();
        } catch (Throwable t) {
            LOG.error("dropTable failed: ", t);
            return ERR;
        }
    }

    /**
     * No-op. MySQL will send dropTable requests for each AkibaDB table in this schema anyway, so we don't need
     * to do anything here.
     * @param schemaName the schema name
     * @return <tt>OK</tt>, in the current implementation
     */
    @Override
    public int dropSchema(final String schemaName) {
        return OK;
//        for (final RowDef rowDef : getRowDefCache().getRowDefs()) {
//            if (rowDef.getSchemaName().equals(schemaName)) {
//                dropTable(rowDef.getRowDefId());
//            }
//        }
//        return OK;
    }

    @Override
    public long getAutoIncrementValue(final int rowDefId) throws Exception {
        final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
        final Exchange exchange;
        final RowDef groupRowDef = rowDef.isGroupTable() ? rowDef : rowDefCache
                .getRowDef(rowDef.getGroupRowDefId());

        final String treeName = groupRowDef.getTreeName();

        switch (rowDef.getRowType()) {
        case GROUP:
            return -1L;
        case ROOT:
            exchange = db.getExchange(VOLUME_NAME, treeName, true);
            exchange.append(rowDef.getOrdinal());
            break;
        case CHILD:
        case GRANDCHILD:
            exchange = db
                    .getExchange(VOLUME_NAME, rowDef.getPkTreeName(), true);
            break;
        default:
            throw new AssertionError("MissingCase");
        }
        exchange.getKey().append(Key.AFTER);
        boolean found = exchange.previous();
        long value = -1;
        if (found) {
            final Class<?> clazz = exchange.getKey().indexTo(-1).decodeType();
            if (clazz == Long.class) {
                value = exchange.getKey().decodeLong();
            }
        }
        releaseExchange(exchange);
        return value;
    }

    @Override
    public RowCollector getCurrentRowCollector(final int tableId) {
        Map<Integer, RowCollector> map = sessionRowCollectorMap.get(Thread
                .currentThread());
        if (map == null) {
            return null;
        }
        return map.get(tableId);
    }

    public void putCurrentRowCollector(final int tableId, final RowCollector rc) {
        Map<Integer, RowCollector> map = sessionRowCollectorMap.get(Thread
                .currentThread());
        if (map == null) {
            map = new HashMap<Integer, RowCollector>();
            sessionRowCollectorMap.put(Thread.currentThread(), map);
        }
        map.put(tableId, rc);
    }

    public void removeCurrentRowCollector(final int tableId) {
        Map<Integer, RowCollector> map = sessionRowCollectorMap.get(Thread
                .currentThread());
        if (map != null) {
            map.remove(tableId);
        }
    }

    public final RowDef checkRequest(int rowDefId, RowData start, RowData end,
            int indexId, int scanFlags) throws Exception {
        final TableStatus ts = tableManager.getTableStatus(rowDefId);

        if (ts.isDeleted()) {
            throw new StoreException(HA_ERR_NO_SUCH_TABLE, "Table "
                    + getRowDefCache().getRowDef(rowDefId).getTableName()
                    + " has been deleted");
        }

        if (start != null && start.getRowDefId() != rowDefId) {
            throw new IllegalArgumentException(
                    "Start and end RowData must specify the same rowDefId");
        }

        if (end != null && end.getRowDefId() != rowDefId) {
            throw new IllegalArgumentException(
                    "Start and end RowData must specify the same rowDefId");
        }
        return rowDefCache.getRowDef(rowDefId);
    }

    public RowCollector newRowCollector(ScanRowsRequest request)
            throws Exception {

        try {
            NEW_COLLECTOR_TAP.in();

            int rowDefId = request.getTableId();
            RowData start = request.getStart();
            RowData end = request.getEnd();
            int indexId = request.getIndexId();
            int scanFlags = request.getScanFlags();
            byte[] columnBitMap = request.getColumnBitMap();
            final RowDef rowDef = checkRequest(rowDefId, start, end, indexId,
                    scanFlags);

            RowCollector rc = new PersistitStoreRowCollector(this, scanFlags,
                    start, end, columnBitMap, rowDef, indexId);
            if (rc.hasMore()) {
                putCurrentRowCollector(rowDefId, rc);
            }
            NEW_COLLECTOR_TAP.out();
            return rc;
        } catch (StoreException e) {
            if (verbose && LOG.isInfoEnabled()) {
                LOG.info("updateRow error " + e.getResult(), e);
            }
            throw e;
        }
    }

    @Override
    public RowCollector newRowCollector(final int rowDefId, int indexId,
            final int scanFlags, RowData start, RowData end, byte[] columnBitMap)
            throws Exception {
        try {
            NEW_COLLECTOR_TAP.in();
            final RowDef rowDef = checkRequest(rowDefId, start, end, indexId,
                    scanFlags);
            final RowCollector rc = new PersistitStoreRowCollector(this,
                    scanFlags, start, end, columnBitMap, rowDef, indexId);

            if (rc.hasMore()) {
                putCurrentRowCollector(rowDefId, rc);
            }
            NEW_COLLECTOR_TAP.out();
            return rc;
        } catch (StoreException e) {
            if (verbose && LOG.isInfoEnabled()) {
                LOG.info("updateRow error " + e.getResult(), e);
            }
            throw e;
        }
    }

    @Override
    public long getRowCount(final boolean exact, final RowData start,
            final RowData end, final byte[] columnBitMap) throws Exception {
        //
        // TODO: Compute a reasonable value. The value "2" is a hack -
        // special because it's not 0 or 1, but small enough to induce
        // MySQL to use an index rather than full table scan.
        //
        return 2;
        // final int tableId = start.getRowDefId();
        // final TableStatus status = tableManager.getTableStatus(tableId);
        // return status.getRowCount();
    }

    @Override
    public TableStatistics getTableStatistics(int tableId) throws Exception {
        final RowDef rowDef = rowDefCache.getRowDef(tableId);
        final TableStatistics ts = new TableStatistics(tableId);
        final TableStatus status = tableManager.getTableStatus(tableId);
        if (rowDef.getRowType() == RowType.GROUP) {
            ts.setRowCount(2);
            ts.setAutoIncrementValue(-1);
        } else {
            ts.setAutoIncrementValue(status.getAutoIncrementValue());
            ts.setRowCount(status.getRowCount());
        }
        ts.setUpdateTime(Math.max(status.getLastUpdateTime(), status
                .getLastWriteTime()));
        ts.setCreationTime(status.getCreationTime());
        // TODO - get correct values
        ts.setMeanRecordLength(100);
        ts.setBlockSize(8192);
        indexManager.populateTableStatistics(ts);
        return ts;
    }

    @Override
    public void analyzeTable(final int tableId) throws Exception {
        final RowDef rowDef = rowDefCache.getRowDef(tableId);
        indexManager.analyzeTable(rowDef);
    }

    @Override
    public List<RowData> fetchRows(final String schemaName,
            final String tableName, final String columnName,
            final Object least, final Object greatest,
            final String leafTableName) throws Exception {
        final List<RowData> list = new ArrayList<RowData>();
        final String compoundName = schemaName + "." + tableName;
        final RowDef rowDef = rowDefCache.getRowDef(compoundName);
        if (rowDef == null) {
            throw new StoreException(HA_ERR_NO_REFERENCED_ROW, "Unknown table "
                    + compoundName);
        }

        final RowDef groupRowDef = rowDef.isGroupTable() ? rowDef : rowDefCache
                .getRowDef(rowDef.getGroupRowDefId());
        final RowDef[] userRowDefs = groupRowDef.getUserTableRowDefs();

        FieldDef fieldDef = null;
        for (final FieldDef fd : rowDef.getFieldDefs()) {
            if (fd.getName().equals(columnName)) {
                fieldDef = fd;
                break;
            }
        }
        if (fieldDef == null) {
            throw new StoreException(HA_ERR_NO_REFERENCED_ROW,
                    "Unknown column " + columnName + " in " + compoundName);
        }

        IndexDef indexDef = null;
        for (final IndexDef id : rowDef.getIndexDefs()) {
            if (id.getFields()[0] == fieldDef.getFieldIndex()) {
                indexDef = id;
                if (indexDef.getFields().length == 1) {
                    break;
                }
            }
        }

        if (indexDef == null) {
            throw new StoreException(HA_ERR_NO_REFERENCED_ROW,
                    "No available index on column " + columnName + " in "
                            + compoundName);
        }

        boolean deepMode = false;
        RowDef leafRowDef = null;
        if (tableName.equals(leafTableName)) {
            leafRowDef = rowDef;
        } else if (leafTableName == null) {
            leafRowDef = rowDef;
            deepMode = true;
        } else
            for (int index = 0; index < userRowDefs.length; index++) {
                if (userRowDefs[index].getTableName().equals(leafTableName)) {
                    leafRowDef = userRowDefs[index];
                    break;
                }
            }

        if (leafRowDef == null) {
            throw new StoreException(HA_ERR_NO_REFERENCED_ROW,
                    "No table named " + leafTableName + " in group");
        }

        final Object[] startValues = new Object[groupRowDef.getFieldCount()];
        final Object[] endValues = new Object[groupRowDef.getFieldCount()];
        startValues[fieldDef.getFieldIndex() + rowDef.getColumnOffset()] = least;
        endValues[fieldDef.getFieldIndex() + rowDef.getColumnOffset()] = greatest;
        final RowData start = new RowData(new byte[1024]);
        final RowData end = new RowData(new byte[1024]);
        start.createRow(groupRowDef, startValues);
        end.createRow(groupRowDef, endValues);

        final byte[] bitMap = new byte[(7 + groupRowDef.getFieldCount()) / 8];
        for (RowDef def = leafRowDef; def != null;) {
            final int bit = def.getColumnOffset();
            final int fc = def.getFieldCount();
            for (int i = bit; i < bit + fc; i++) {
                bitMap[i / 8] |= (1 << (i % 8));
            }
            if (def != rowDef && def.getParentRowDefId() != 0) {
                def = rowDefCache.getRowDef(def.getParentRowDefId());
            } else {
                break;
            }
        }

        final RowCollector rc = newRowCollector(groupRowDef.getRowDefId(),
                indexDef.getId(), deepMode ? SCAN_FLAGS_DEEP : 0, start, end,
                bitMap);
        while (rc.hasMore()) {
            final ByteBuffer payload = ByteBuffer.allocate(65536);
            while (rc.collectNextRow(payload))
                ;
            payload.flip();
            for (int p = payload.position(); p < payload.limit();) {
                final RowData rowData = new RowData(payload.array(), p, payload
                        .limit());
                rowData.prepareRow(p);
                list.add(rowData);
                p = rowData.getRowEnd();
            }
        }
        rc.close();
        return list;
    }

    // ---------------------------------
    void insertIntoIndex(final IndexDef indexDef, final RowData rowData,
            final Key hkey, final boolean deferIndexes) throws Exception {
        final Exchange iEx = getExchange(indexDef.getRowDef(), indexDef);
        constructIndexKey(iEx.getKey(), rowData, indexDef, hkey);
        final Key key = iEx.getKey();

        if (indexDef.isUnique()) {
            int saveSize = key.getEncodedSize();
            key.setDepth(indexDef.getIndexKeySegmentCount());
            if (iEx.hasChildren()) {
                throw new StoreException(HA_ERR_FOUND_DUPP_KEY,
                        "Non-unique index key: " + key.toString());
            }
            key.setEncodedSize(saveSize);
        }
        iEx.getValue().clear();
        if (deferIndexes) {
            synchronized (deferredIndexKeys) {
                final String treeName = iEx.getTree().getName();
                SortedSet<KeyState> keySet = deferredIndexKeys.get(treeName);
                if (keySet == null) {
                    keySet = new TreeSet<KeyState>();
                    deferredIndexKeys.put(treeName, keySet);
                }
                final KeyState ks = new KeyState(iEx.getKey());
                keySet.add(ks);
                deferredIndexKeyLimit -= (ks.getBytes().length + KEY_STATE_SIZE_OVERHEAD);
            }
        } else {
            iEx.store();
        }
    }

    void putAllDeferredIndexKeys() throws PersistitException {
        synchronized (deferredIndexKeys) {
            for (final Map.Entry<String, SortedSet<KeyState>> entry : deferredIndexKeys
                    .entrySet()) {
                final Exchange iEx = getExchange(entry.getKey());
                buildIndexAddKeys(entry.getValue(), iEx);
                entry.getValue().clear();
            }
            deferredIndexKeyLimit = MAX_INDEX_TRANCHE_SIZE;
        }
    }

    void updateIndex(final IndexDef indexDef, final RowDef rowDef,
            final RowData oldRowData, final RowData newRowData, final Key hkey)
            throws Exception {

        if (!fieldsEqual(rowDef, oldRowData, newRowData, indexDef.getFields())) {
            final Exchange oldExchange = getExchange(rowDef, indexDef);
            constructIndexKey(oldExchange.getKey(), oldRowData, indexDef, hkey);
            final Exchange newExchange = getExchange(rowDef, indexDef);
            constructIndexKey(newExchange.getKey(), newRowData, indexDef, hkey);

            oldExchange.getValue().clear();
            newExchange.getValue().clear();

            oldExchange.remove();
            newExchange.store();

            releaseExchange(newExchange);
            releaseExchange(oldExchange);
        }
    }

    void deleteIndex(final IndexDef indexDef, final RowDef rowDef,
            final RowData rowData, final Key hkey) throws Exception {
        final Exchange iEx = getExchange(rowDef, indexDef);
        constructIndexKey(iEx.getKey(), rowData, indexDef, hkey);
        iEx.remove();
        releaseExchange(iEx);
    }

    boolean bytesEqual(final byte[] a, final int aoffset, final int asize,
            final byte[] b, final int boffset, final int bsize) {
        if (asize != bsize) {
            return false;
        }
        for (int i = 0; i < asize; i++) {
            if (a[i + aoffset] != b[i + boffset]) {
                return false;
            }
        }
        return true;
    }

    boolean fieldsEqual(final RowDef rowDef, final RowData a, final RowData b,
            final int[] fieldIndexes) {
        for (int index = 0; index < fieldIndexes.length; index++) {
            final int fieldIndex = fieldIndexes[index];
            final long aloc = rowDef.fieldLocation(a, fieldIndex);
            final long bloc = rowDef.fieldLocation(b, fieldIndex);
            if (!bytesEqual(a.getBytes(), (int) aloc, (int) (aloc >>> 32), b
                    .getBytes(), (int) bloc, (int) (bloc >>> 32))) {
                return false;
            }
        }
        return true;
    }

    void expandRowData(final Exchange exchange, final int expectedRowDefId,
            final RowData rowData) throws StoreException {
        final int size = exchange.getValue().getEncodedSize();
        final int rowDataSize = size + RowData.ENVELOPE_SIZE;
        final byte[] valueBytes = exchange.getValue().getEncodedBytes();
        byte[] rowDataBytes = rowData.getBytes();

        if (rowDataSize < RowData.MINIMUM_RECORD_LENGTH
                || rowDataSize > RowData.MAXIMUM_RECORD_LENGTH) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Value at " + exchange.getKey()
                        + " is not a valid row - skipping");
            }
            throw new StoreException(HA_ERR_INTERNAL_ERROR,
                    "Corrupt RowData at " + exchange.getKey());
        }

        final int rowDefId = CServerUtil.getInt(valueBytes,
                RowData.O_ROW_DEF_ID - RowData.LEFT_ENVELOPE_SIZE);

        if (rowDefId != expectedRowDefId && expectedRowDefId != 0) {
            //
            // Add code to here to evolve data to required expectedRowDefId
            //
            throw new StoreException(HA_ERR_INTERNAL_ERROR,
                    "Unable to convert rowDefId " + rowDefId
                            + " to expected rowDefId " + expectedRowDefId);
        }
        if (rowDataSize > rowDataBytes.length) {
            rowDataBytes = new byte[rowDataSize + INITIAL_BUFFER_SIZE];
            rowData.reset(rowDataBytes);
        }

        //
        // Assemble the Row in a byte array to allow column
        // elision
        //
        CServerUtil.putInt(rowDataBytes, RowData.O_LENGTH_A, rowDataSize);
        CServerUtil.putChar(rowDataBytes, RowData.O_SIGNATURE_A,
                RowData.SIGNATURE_A);
        System.arraycopy(valueBytes, 0, rowDataBytes, RowData.O_FIELD_COUNT,
                size);
        CServerUtil.putChar(rowDataBytes, RowData.O_SIGNATURE_B + rowDataSize,
                RowData.SIGNATURE_B);
        CServerUtil.putInt(rowDataBytes, RowData.O_LENGTH_B + rowDataSize,
                rowDataSize);
        rowData.prepareRow(0);
    }

    @Override
    public void addCommittedUpdateListener(CommittedUpdateListener listener) {
        updateListeners.add(listener);
    }

    @Override
    public void removeCommittedUpdateListener(CommittedUpdateListener listener) {
        updateListeners.remove(listener);
    }

    public void buildIndexes(final String ddl) {
        flushIndexes();

        final Set<RowDef> userRowDefs = new HashSet<RowDef>();
        final Set<RowDef> groupRowDefs = new HashSet<RowDef>();

        // Find the groups containing indexes selected for rebuild.
        for (final RowDef rowDef : rowDefCache.getRowDefs()) {
            if (!rowDef.isGroupTable()) {
                for (final IndexDef indexDef : rowDef.getIndexDefs()) {
                    if (isIndexSelected(indexDef, ddl)) {
                        userRowDefs.add(rowDef);
                        final RowDef group = rowDefCache.getRowDef(rowDef
                                .getGroupRowDefId());
                        if (group != null) {
                            groupRowDefs.add(group);
                        }
                    }
                }
            }
        }

        for (final RowDef rowDef : groupRowDefs) {
            final RowData rowData = new RowData(new byte[MAX_ROW_SIZE]);
            rowData.createRow(rowDef, new Object[0]);

            final byte[] columnBitMap = new byte[(rowDef.getFieldCount() + 7) / 8];
            // Project onto all columns of selected user tables
            for (final RowDef user : rowDef.getUserTableRowDefs()) {
                if (userRowDefs.contains(user)) {
                    for (int bit = 0; bit < user.getFieldCount(); bit++) {
                        final int c = bit + user.getColumnOffset();
                        columnBitMap[c / 8] |= (1 << (c % 8));
                    }
                }
            }
            int indexKeyCount = 0;
            try {
                final PersistitStoreRowCollector rc = (PersistitStoreRowCollector) newRowCollector(
                        rowDef.getRowDefId(), 0, 0, rowData, rowData,
                        columnBitMap);
                // final KeyFilter hFilter = rc.getHFilter();
                final Exchange hEx = rc.getHExchange();

                hEx.getKey().clear();
                // while (hEx.traverse(Key.GT, hFilter, Integer.MAX_VALUE)) {
                while (hEx.next(true)) {
                    expandRowData(hEx, 0, rowData);
                    final int tableId = rowData.getRowDefId();
                    final RowDef userRowDef = rowDefCache.getRowDef(tableId);
                    if (userRowDefs.contains(userRowDef)) {
                        for (final IndexDef indexDef : userRowDef
                                .getIndexDefs()) {
                            if (isIndexSelected(indexDef, ddl)) {
                                insertIntoIndex(indexDef, rowData,
                                        hEx.getKey(), true);
                                indexKeyCount++;
                            }
                        }
                        if (deferredIndexKeyLimit <= 0) {
                            putAllDeferredIndexKeys();
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Exception while trying to index group table "
                        + rowDef.getSchemaName() + "." + rowDef.getTableName(),
                        e);
            }
            flushIndexes();
            if (LOG.isInfoEnabled()) {
                LOG.info("Inserted " + indexKeyCount
                        + " index keys into group " + rowDef.getSchemaName()
                        + "." + rowDef.getTableName());
            }
        }
    }

    public void flushIndexes() {
        try {
            putAllDeferredIndexKeys();
        } catch (Exception e) {
            LOG.error("Exception while trying "
                    + " to flush deferred index keys", e);
        }
    }

    public void deleteIndexes(final String ddl) {
        for (final RowDef rowDef : rowDefCache.getRowDefs()) {
            if (!rowDef.isGroupTable()) {
                for (final IndexDef indexDef : rowDef.getIndexDefs()) {
                    if (isIndexSelected(indexDef, ddl)) {
                        try {
                            final Exchange iEx = getExchange(rowDef, indexDef);
                            iEx.removeAll();
                        } catch (Exception e) {
                            LOG.error(
                                    "Exception while trying to remove index tree "
                                            + indexDef.getTreeName(), e);
                        }
                    }
                }
            }
        }
    }

    private boolean isIndexSelected(final IndexDef indexDef, final String ddl) {
        return !indexDef.isHKeyEquivalent()
                && (ddl.contains("table=(" + indexDef.getRowDef().getTableName() + ")") || !ddl.contains("table="))
                && (ddl.contains("index=(" + indexDef.getName() + ")") || !ddl.contains("index="));
    }

    private void buildIndexAddKeys(final SortedSet<KeyState> keys,
            final Exchange iEx) throws PersistitException {
        final long start = System.nanoTime();
        for (final KeyState keyState : keys) {
            keyState.copyTo(iEx.getKey());
            iEx.store();
        }
        final long elapsed = System.nanoTime() - start;
        if (LOG.isInfoEnabled()) {
            LOG.info(String.format("Index builder inserted %s keys "
                    + "into index tree %s in %,d seconds", keys.size(), iEx
                    .getTree().getName(), elapsed / 1000000000));
        }
    }

    public boolean isDeferIndexes() {
        return deferIndexes;
    }

    public void setDeferIndexes(final boolean defer) {
        deferIndexes = defer;
    }
}

