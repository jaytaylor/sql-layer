package com.akiban.cserver.store;

import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_DEEP;
import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_END_AT_EDGE;
import static com.akiban.cserver.store.RowCollector.SCAN_FLAGS_START_AT_EDGE;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.HKeyColumn;
import com.akiban.ais.model.HKeySegment;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.IndexDef;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.RowType;
import com.akiban.cserver.TableStatistics;
import com.akiban.cserver.message.ScanRowsRequest;
import com.akiban.cserver.service.persistit.PersistitService;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.message.ErrorCode;
import com.akiban.util.Tap;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.Transaction.CommitListener;
import com.persistit.TransactionRunnable;
import com.persistit.Tree;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TransactionFailedException;

public class PersistitStore implements CServerConstants, Store {

    final static int INITIAL_BUFFER_SIZE = 1024;

    private static final Log LOG = LogFactory.getLog(PersistitStore.class
            .getName());

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

    private final static int MEGA = 1024 * 1024;

    private final static int MAX_ROW_SIZE = 5000000;

    private final static int MAX_INDEX_TRANCHE_SIZE = 10 * MEGA;

    private final static int KEY_STATE_SIZE_OVERHEAD = 50;

    private boolean verbose = false;

    private final String experimental = "schema";

    private boolean deferIndexes = false;

    private PersistitService ps;

    private final RowDefCache rowDefCache;

    private PersistitStoreTableManager tableManager;

    private PersistitStoreIndexManager indexManager;

    private PersistitStoreSchemaManager schemaManager;

    private boolean forceToDisk = false; // default to "group commit"

    private List<CommittedUpdateListener> updateListeners = Collections
            .synchronizedList(new ArrayList<CommittedUpdateListener>());

    private final Map<Tree, SortedSet<KeyState>> deferredIndexKeys = new HashMap<Tree, SortedSet<KeyState>>();

    private int deferredIndexKeyLimit = MAX_INDEX_TRANCHE_SIZE;

    public PersistitStore(final PersistitService ps) {
        this.ps = ps;
        this.rowDefCache = new RowDefCache();
    }

    private synchronized void createManagers() {
        this.tableManager = new PersistitStoreTableManager(this);
        this.indexManager = new PersistitStoreIndexManager(this);
        this.schemaManager = new PersistitStoreSchemaManager(this);
    }

    public synchronized void start() throws Exception {
        createManagers();
        tableManager.startUp();
        schemaManager.startUp();
    }

    private synchronized void destroyManagers() throws Exception {
        tableManager.shutDown();
        tableManager = null;
        indexManager.shutDown();
        indexManager = null;
    }

    public synchronized void stop() throws Exception {
        destroyManagers();
    }

    @Override
    public Store cast() {
        return this;
    }

    @Override
    public Class<Store> castClass() {
        return Store.class;
    }

    public Persistit getDb() {
        return ps.getDb();
    }

    public SchemaId getSchemaId() throws Exception {
        return schemaManager.getSchemaID();
    }

    @Override
    public AkibaInformationSchema getAis() {
        return schemaManager.getAis();
    }

    public Exchange getExchange(final Session session, final RowDef rowDef,
            final IndexDef indexDef) throws PersistitException {
        final String treeName;
        if (indexDef == null) {
            final RowDef groupRowDef = rowDef.isGroupTable() ? rowDef
                    : rowDefCache.getRowDef(rowDef.getGroupRowDefId());
            treeName = groupRowDef.getTreeName();
        } else {
            treeName = indexDef.getTreeName();
        }
        return ps.getExchange(session, rowDef.getSchemaName(), treeName);
    }

    public void releaseExchange(final Session session, final Exchange exchange) {
        ps.releaseExchange(session, exchange);
    }

    private Exchange getExchange(final Session session, final Tree tree)
            throws PersistitException {
        return ps.getExchange(session, tree);
    }

    public Exchange getExchange(final Session session, final String treeName)
            throws PersistitException {
        return ps.getExchange(session, "_system_", treeName);
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
     * @throws PersistitException
     * @throws InvalidOperationException
     */
    void constructHKey(final Session session, Exchange hEx, RowDef rowDef,
            RowData rowData) throws PersistitException,
            InvalidOperationException {
        /*
         * constructHKeyOld(hEx, rowDef, rowData); String oldHKey =
         * hEx.getKey().toString();
         */
        constructHKeyNew(session, hEx, rowDef, rowData);
        /*
         * String newHKey = hEx.getKey().toString(); assert
         * oldHKey.equals(newHKey) : String.format("old: %s, new: %s", oldHKey,
         * newHKey);
         */
    }

    void constructHKeyOld(final Session session, final Exchange hEx,
            final RowDef rowDef, final RowData rowData)
            throws PersistitException, InvalidOperationException {
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
            RowDef parentRowDef = rowDefCache.getRowDef(rowDef
                    .getParentRowDefId());
            hkey.append(parentRowDef.getOrdinal());
            appendKeyFields(hkey, rowDef, rowData, rowDef.getParentJoinFields());
            if (!hEx.isValueDefined()) {
                throw new InvalidOperationException(
                        ErrorCode.NO_REFERENCED_ROW, hkey.toString());
            }
            hkey.append(rowDef.getOrdinal());
            appendKeyFields(hkey, rowDef, rowData, rowDef.getPkFields());
            break;
        }

        case GRANDCHILD: {
            RowDef parentRowDef = rowDefCache.getRowDef(rowDef
                    .getParentRowDefId());
            Exchange iEx = getExchange(session, rowDef,
                    parentRowDef.getPKIndexDef());
            constructParentPKIndexKey(iEx.getKey(), rowDef, rowData);
            if (!iEx.hasChildren()) {
                throw new InvalidOperationException(
                        ErrorCode.NO_REFERENCED_ROW, iEx.getKey().toString());
            }
            boolean next = iEx.next(true);
            assert next;
            constructHKeyFromIndexKey(hkey, iEx.getKey(),
                    parentRowDef.getPKIndexDef());
            hkey.append(rowDef.getOrdinal());
            appendKeyFields(hkey, rowDef, rowData, rowDef.getPkFields());
            releaseExchange(session, iEx);
        }
        }
    }

    void constructHKeyNew(final Session session, Exchange hEx, RowDef rowDef,
            RowData rowData) throws PersistitException,
            InvalidOperationException {
        Key hKey = hEx.getKey();
        hKey.clear();
        UserTable table = rowDef.userTable();
        int parentHKeySegments = 0;
        if (!table.containsOwnHKey()) {
            // For hkey fields not in rowDef's table, find the parent row and
            // get the hkey from it. That provides
            // the hkey up to the point that rowData can contribute the rest.
            RowDef parentRowDef = rowDefCache.getRowDef(rowDef
                    .getParentRowDefId());
            Exchange iEx = getExchange(session, rowDef,
                    parentRowDef.getPKIndexDef());
            constructParentPKIndexKey(iEx.getKey(), rowDef, rowData);
            if (!iEx.hasChildren()) {
                throw new InvalidOperationException(
                        ErrorCode.NO_REFERENCED_ROW, iEx.getKey().toString());
            }
            boolean next = iEx.next(true);
            assert next;
            constructHKeyFromIndexKey(hKey, iEx.getKey(),
                    parentRowDef.getPKIndexDef());
            releaseExchange(session, iEx);
            parentHKeySegments = parentRowDef.userTable().hKey().segments()
                    .size();
        }
        // Get hkey contributions from this row
        List<HKeySegment> segments = table.hKey().segments();
        for (int s = parentHKeySegments; s < segments.size(); s++) {
            HKeySegment segment = segments.get(s);
            RowDef segmentRowDef = rowDefCache.getRowDef(segment.table()
                    .getTableId());
            hKey.append(segmentRowDef.getOrdinal());
            FieldDef[] fieldDefs = rowDef.getFieldDefs();
            List<HKeyColumn> segmentColumns = segment.columns();
            for (int c = 0; c < segmentColumns.size(); c++) {
                HKeyColumn segmentColumn = segmentColumns.get(c);
                appendKeyField(hKey, fieldDefs[segmentColumn.column()
                        .getPosition()], rowData);
            }
        }
    }

    void constructHKey(Exchange hEx, RowDef rowDef, int[] ordinals,
            int[] nKeyColumns, FieldDef[] hKeyFieldDefs, Object[] hKeyValues)
            throws Exception {
        final Key hkey = hEx.getKey();
        hkey.clear();
        int k = 0;
        for (int i = 0; i < ordinals.length; i++) {
            hkey.append(ordinals[i]);
            for (int j = 0; j < nKeyColumns[i]; j++) {
                appendKeyField(hkey, hKeyFieldDefs[k], hKeyValues[k]);
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
    void constructIndexKey(Key iKey, RowData rowData, IndexDef indexDef,
            Key hKey) throws PersistitException {
        IndexDef.H2I[] fassoc = indexDef.indexKeyFields();
        iKey.clear();
        for (int index = 0; index < fassoc.length; index++) {
            IndexDef.H2I assoc = fassoc[index];
            if (assoc.fieldIndex() >= 0) {
                int fieldIndex = assoc.fieldIndex();
                RowDef rowDef = indexDef.getRowDef();
                appendKeyField(iKey, rowDef.getFieldDef(fieldIndex), rowData);
            } else if (assoc.hKeyLoc() >= 0) {
                appendKeyFieldFromKey(hKey, iKey, assoc.hKeyLoc());
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
        final IndexDef.I2H[] fassoc = indexDef.hkeyFields();
        hKey.clear();
        for (int index = 0; index < fassoc.length; index++) {
            final IndexDef.I2H fa = fassoc[index];
            if (fa.isOrdinalType()) {
                hKey.append(fa.ordinal());
            } else {
                final int depth = fassoc[index].indexKeyLoc();
                if (depth < 0 || depth > indexKey.getDepth()) {
                    throw new IllegalStateException(
                            "IndexKey too shallow - requires depth=" + depth
                                    + ": " + indexKey);
                }
                appendKeyFieldFromKey(indexKey, hKey, fa.indexKeyLoc());
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
            System.arraycopy(fromKey.getEncodedBytes(), from,
                    toKey.getEncodedBytes(), toKey.getEncodedSize(), to - from);
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
            throw new NullPointerException(description
                    + " is null; did you call startUp()?");
        }
        return object;
    }

    public TableManager getTableManager() {
        return errorIfNull("table manager", tableManager);
    }

    public IndexManager getIndexManager() {
        return errorIfNull("index manager", indexManager);
    }

    public PersistitStoreSchemaManager getSchemaManager() {
        return errorIfNull("schema manger", schemaManager);
    }

    public void fixUpOrdinals() throws Exception {
        rowDefCache.fixUpOrdinals(tableManager);
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

    /**
     * Tries to create the table
     * 
     * @param schemaName
     *            the table's schema
     * @param ddl
     *            the table's ddl
     * @throws InvalidOperationException
     *             if the table is invalid
     */
    @Override
    public void createTable(final Session session, final String schemaName,
            final String ddl) throws PersistitException,
            InvalidOperationException {
        if (AKIBAN_SPECIAL_SCHEMA_FLAG.equals(schemaName)) {
            deferIndexes = ddl.contains(AKIBAN_SPECIAL_DEFER_INDEXES_FLAG);
            if (ddl.contains(AKIBAN_SPECIAL_FLUSH_INDEXES_FLAG)) {
                flushIndexes(session);
            }
            if (ddl.contains(AKIBAN_SPECIAL_DELETE_INDEXES_FLAG)) {
                deleteIndexes(session, ddl);
            }
            if (ddl.contains(AKIBAN_SPECIAL_BUILD_INDEXES_FLAG)) {
                buildIndexes(session, ddl);
            }
            return;
        }
        Transaction transaction = ps.getTransaction();
        try {
            final CreateTableResult result = new CreateTableResult();
            transaction.run(new TransactionRunnable() {
                @Override
                public void runTransaction() {
                    try {
                        schemaManager.createTable(session, schemaName, ddl,
                                rowDefCache, result);
                    } catch (Exception e) {
                        throw new RollbackException(e);
                    }
                }
            });
            if (!result.wasSuccessful()) {
                throw new InvalidOperationException(ErrorCode.UNKNOWN,
                        "Result failed: " + result + " for <" + schemaName
                                + "> " + ddl);
            }
            TableStatus ts = tableManager.getTableStatus(result.getTableId());
            ts.reset();
            if (result.autoIncrementDefined()) {
                ts.setAutoIncrementValue(result.defaultAutoIncrement());
            }
        } catch (RollbackException e) {
            Throwable cause = e.getCause();
            if (cause instanceof InvalidOperationException) {
                throw (InvalidOperationException) cause;
            }
            throw e;
        }
    }

    private TableStatus checkTableStatus(int rowDefId)
            throws PersistitException, InvalidOperationException {
        TableStatus ts = tableManager.getTableStatus(rowDefId);
        if (ts.isDeleted()) {
            throw new InvalidOperationException(ErrorCode.NO_SUCH_TABLE,
                    "Table is deleted: %d", rowDefId);
        }
        return ts;
    }

    /**
     * WRites a row
     * 
     * @param rowData
     *            the row data
     * @throws InvalidOperationException
     *             if the given table is unknown or deleted; or if there's a
     *             duplicate key error
     */
    @Override
    public void writeRow(final Session session, final RowData rowData)
            throws InvalidOperationException, PersistitException {
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
            hEx = getExchange(session, rowDef, null);
            transaction = ps.getTransaction();
            int retries = MAX_TRANSACTION_RETRY_COUNT;
            for (;;) {
                transaction.begin();
                try {
                    final TableStatus ts = checkTableStatus(rowDefId);

                    //
                    // Does the heavy lifting of looking up the full hkey in
                    // parent's primary index if necessary.
                    //
                    constructHKey(session, hEx, rowDef, rowData);

                    if (hEx.isValueDefined()) {
                        throw new InvalidOperationException(
                                ErrorCode.DUPLICATE_KEY, "Non-unique key: %s",
                                hEx.getKey());
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
                            insertIntoIndex(session, indexDef, rowData,
                                    hEx.getKey(), deferIndexes);
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
                putAllDeferredIndexKeys(session);
            }
            return;
        } finally {
            releaseExchange(session, hEx);
            WRITE_ROW_TAP.out();
        }
    }

    @Override
    public void writeRowForBulkLoad(final Session session, Exchange hEx,
            RowDef rowDef, RowData rowData, int[] ordinals, int[] nKeyColumns,
            FieldDef[] hKeyFieldDefs, Object[] hKeyValues) throws Exception {
        /*
         * if (verbose && LOG.isInfoEnabled()) { LOG.info("BulkLoad writeRow: "
         * + rowData.toString(rowDefCache)); }
         */

        constructHKey(hEx, rowDef, ordinals, nKeyColumns, hKeyFieldDefs,
                hKeyValues);
        final int start = rowData.getInnerStart();
        final int size = rowData.getInnerSize();
        hEx.getValue().ensureFit(size);
        System.arraycopy(rowData.getBytes(), start, hEx.getValue()
                .getEncodedBytes(), 0, size);
        hEx.getValue().setEncodedSize(size);
        // Store the h-row
        hEx.store();
        /*
         * for (final IndexDef indexDef : rowDef.getIndexDefs()) { // Insert the
         * index keys (except for the case of a // root table's PK index.) if
         * (!indexDef.isHKeyEquivalent()) { insertIntoIndex(indexDef, rowData,
         * hEx.getKey(), deferIndexes); } } if (deferredIndexKeyLimit <= 0) {
         * putAllDeferredIndexKeys(); }
         */
        return;
    }

    @Override
    public void updateTableStats(final Session session, RowDef rowDef,
            long rowCount) throws InvalidOperationException, PersistitException {
        final int rowDefId = rowDef.getRowDefId();
        TableStatus tableStatus = checkTableStatus(rowDefId);
        tableStatus.setRowCount(rowCount);
        tableStatus.updated();
        tableManager.saveStatus(tableStatus);
    }

    @Override
    public void deleteRow(final Session session, final RowData rowData)
            throws InvalidOperationException, PersistitException {
        DELETE_ROW_TAP.in();
        final int rowDefId = rowData.getRowDefId();

        final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
        Exchange hEx = null;

        try {
            hEx = getExchange(session, rowDef, null);

            final Transaction transaction = ps.getTransaction();
            int retries = MAX_TRANSACTION_RETRY_COUNT;
            for (;;) {
                transaction.begin();
                try {
                    final TableStatus ts = checkTableStatus(rowDefId);

                    constructHKey(session, hEx, rowDef, rowData);
                    hEx.fetch();
                    //
                    // Verify that the row exists
                    //
                    if (!hEx.getValue().isDefined()) {
                        throw new InvalidOperationException(
                                ErrorCode.NO_SUCH_RECORD,
                                "Missing record at key: %s", hEx.getKey());
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
                        throw new InvalidOperationException(
                                ErrorCode.FK_CONSTRAINT_VIOLATION,
                                "Can't cascade DELETE: %s", hEx.getKey());

                    }

                    // Remove the h-row
                    hEx.remove();
                    ts.incrementRowCount(-1);
                    ts.deleted();
                    // Remove the indexes, including the PK index
                    for (final IndexDef indexDef : rowDef.getIndexDefs()) {
                        if (!indexDef.isHKeyEquivalent()) {
                            deleteIndex(session, indexDef, rowDef, rowData,
                                    hEx.getKey());
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
                    return;
                } catch (RollbackException re) {
                    if (--retries < 0) {
                        throw new TransactionFailedException();
                    }
                } finally {
                    transaction.end();
                }
            }
        } finally {
            releaseExchange(session, hEx);
            DELETE_ROW_TAP.out();
        }
    }

    @Override
    public void updateRow(final Session session, final RowData oldRowData,
            final RowData newRowData) throws InvalidOperationException,
            PersistitException {
        final int rowDefId = oldRowData.getRowDefId();

        if (newRowData.getRowDefId() != rowDefId) {
            throw new IllegalArgumentException(
                    "RowData values have different rowDefId values: ("
                            + rowDefId + "," + newRowData.getRowDefId() + ")");
        }
        UPDATE_ROW_TAP.in();
        final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
        Exchange hEx = null;
        try {
            hEx = getExchange(session, rowDef, null);
            final Transaction transaction = ps.getTransaction();
            int retries = MAX_TRANSACTION_RETRY_COUNT;
            for (;;) {
                transaction.begin();
                try {
                    final TableStatus ts = checkTableStatus(rowDefId);
                    constructHKey(session, hEx, rowDef, oldRowData);
                    hEx.fetch();
                    //
                    // Verify that the row exists
                    //
                    if (!hEx.getValue().isDefined()) {
                        throw new InvalidOperationException(
                                ErrorCode.NO_SUCH_RECORD,
                                "Missing record at key: %s", hEx.getKey());
                    }
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
                            throw new InvalidOperationException(
                                    ErrorCode.FK_CONSTRAINT_VIOLATION,
                                    "Can't cascade UPDATE on PK field: %s",
                                    hEx.getKey());
                        }
                        deleteRow(session, oldRowData);
                        writeRow(session, newRowData);
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
                                updateIndex(session, indexDef, rowDef,
                                        oldRowData, newRowData, hEx.getKey());
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
                    return;
                } catch (RollbackException re) {
                    if (--retries < 0) {
                        throw new TransactionFailedException();
                    }
                } finally {
                    transaction.end();
                }
            }
        } finally {
            releaseExchange(session, hEx);
            UPDATE_ROW_TAP.out();
        }
    }

    /**
     * Remove contents of table. TODO: remove user table data from within a
     * group.
     */
    @Override
    public void truncateTable(final Session session, final int rowDefId)
            throws PersistitException, InvalidOperationException {

        final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
        Transaction transaction = null;

        RowDef groupRowDef = rowDef.isGroupTable() ? rowDef : rowDefCache
                .getRowDef(rowDef.getGroupRowDefId());

        transaction = ps.getTransaction();
        int retries = MAX_TRANSACTION_RETRY_COUNT;
        for (;;) {

            transaction.begin();

            try {
                final TableStatus ts = checkTableStatus(rowDefId);
                //
                // Remove the index trees
                //
                for (final IndexDef indexDef : groupRowDef.getIndexDefs()) {
                    if (!indexDef.isHKeyEquivalent()) {
                        final Exchange iEx = getExchange(session, groupRowDef,
                                indexDef);
                        iEx.removeAll();
                        releaseExchange(session, iEx);
                    }
                    indexManager.deleteIndexAnalysis(session, indexDef);
                }
                for (final RowDef userRowDef : groupRowDef
                        .getUserTableRowDefs()) {
                    for (final IndexDef indexDef : userRowDef.getIndexDefs()) {
                        indexManager.deleteIndexAnalysis(session, indexDef);
                    }
                }
                //
                // remove the htable tree
                //
                final Exchange hEx = getExchange(session, groupRowDef, null);
                hEx.removeAll();
                releaseExchange(session, hEx);
                for (int i = 0; i < groupRowDef.getUserTableRowDefs().length; i++) {
                    final int childId = groupRowDef.getUserTableRowDefs()[i]
                            .getRowDefId();
                    final TableStatus ts1 = tableManager
                            .getTableStatus(childId);
                    ts1.setRowCount(Long.MIN_VALUE);
                    ts1.deleted();
                }
                final TableStatus ts0 = tableManager.getTableStatus(groupRowDef
                        .getRowDefId());
                ts0.setRowCount(0);
                ts0.deleted();

                transaction.commit(forceToDisk);
                return;
            } catch (RollbackException re) {
                if (--retries < 0) {
                    throw new TransactionFailedException();
                }
            } finally {
                transaction.end();
            }
        }
    }

    @Override
    public void dropTable(final Session session, int rowDefId)
            throws InvalidOperationException, PersistitException {
        List<Integer> list = new ArrayList<Integer>();
        list.add(rowDefId);
        dropTables(session, list);
    }

    /**
     * Remove contents of table and its schema information. TODO: remove user
     * table data from within a group. clients.
     */
    @Override
    public void dropTables(final Session session,
            final Collection<Integer> rowDefIds)
            throws InvalidOperationException, PersistitException {
        List<Integer> myRowDefIds = new ArrayList(rowDefIds);
        // Never drop group tables
        for (Iterator<Integer> iter = myRowDefIds.iterator(); iter.hasNext();) {
            int rowDefId = iter.next();
            RowDef rowDef = rowDefCache.getRowDef(rowDefId);
            if (rowDef.isGroupTable()) {
                iter.remove();
            }
        }
        if (myRowDefIds.isEmpty()) {
            return;
        }

        final Transaction transaction = ps.getTransaction();
        int retries = MAX_TRANSACTION_RETRY_COUNT;
        final List<String> tablesToForget = new ArrayList<String>();
        for (;;) {
            transaction.begin();
            try {
                tablesToForget.clear();
                for (final int rowDefId : myRowDefIds) {
                    final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
                    schemaManager.dropCreateTable(session,
                            rowDef.getSchemaName(), rowDef.getTableName());
                    tablesToForget.add(rowDef.getTableName());
                    final TableStatus ts = tableManager
                            .getTableStatus(rowDefId);
                    // if (ts.isDeleted()) {
                    // throw new StoreException(HA_ERR_NO_SUCH_TABLE,
                    // "Table "
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
                        schemaManager.dropCreateTable(session,
                                groupRowDef.getSchemaName(),
                                groupRowDef.getTableName());
                        tableManager.getTableStatus(groupRowDef.getRowDefId())
                                .setDeleted(true);
                        // IOException
                        // Remove the index trees
                        //
                        for (final IndexDef indexDef : groupRowDef
                                .getIndexDefs()) {
                            if (!indexDef.isHKeyEquivalent()) {
                                final Exchange iEx = getExchange(session,
                                        groupRowDef, indexDef);
                                iEx.removeAll();
                                releaseExchange(session, iEx);
                            }
                            indexManager.deleteIndexAnalysis(session, indexDef);
                        }
                        for (final RowDef userRowDef : groupRowDef
                                .getUserTableRowDefs()) {
                            for (final IndexDef indexDef : userRowDef
                                    .getIndexDefs()) {
                                indexManager.deleteIndexAnalysis(session,
                                        indexDef);
                            }
                        }
                        //
                        // remove the htable tree
                        //
                        final Exchange hEx = getExchange(session, groupRowDef,
                                null);
                        hEx.removeAll();
                        releaseExchange(session, hEx);

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
                return;
            } catch (RollbackException e) {
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
    }

    /**
     * No-op. MySQL will send dropTable requests for each AkibaDB table in this
     * schema anyway, so we don't need to do anything here.
     * 
     * @param schemaName
     *            the schema name
     * @return <tt>OK</tt>, in the current implementation
     */
    @Override
    public void dropSchema(final Session session, final String schemaName)
            throws InvalidOperationException, PersistitException {
        List<Integer> dropRowDefIds = new ArrayList<Integer>();
        for (final RowDef rowDef : getRowDefCache().getRowDefs()) {
            if (rowDef.getSchemaName().equals(schemaName)) {
                dropRowDefIds.add(rowDef.getRowDefId());
            }
        }
        dropTables(session, dropRowDefIds);
    }

    // @Override
    // public long getAutoIncrementValue(final int rowDefId)
    // throws InvalidOperationException, PersistitException {
    // final RowDef rowDef = rowDefCache.getRowDef(rowDefId);
    // final Exchange exchange;
    // final RowDef groupRowDef = rowDef.isGroupTable() ? rowDef : rowDefCache
    // .getRowDef(rowDef.getGroupRowDefId());
    //
    // final String treeName = groupRowDef.getTreeName();
    //
    // switch (rowDef.getRowType()) {
    // case GROUP:
    // return -1L;
    // case ROOT:
    // exchange = db.getExchange(VOLUME_NAME, treeName, true);
    // exchange.append(rowDef.getOrdinal());
    // break;
    // case CHILD:
    // case GRANDCHILD:
    // exchange = db
    // .getExchange(VOLUME_NAME, rowDef.getPkTreeName(), true);
    // break;
    // default:
    // throw new AssertionError("MissingCase");
    // }
    // exchange.getKey().append(Key.AFTER);
    // boolean found = exchange.previous();
    // long value = -1;
    // if (found) {
    // final Class<?> clazz = exchange.getKey().indexTo(-1).decodeType();
    // if (clazz == Long.class) {
    // value = exchange.getKey().decodeLong();
    // }
    // }
    // else {
    // UserTable uTable =
    // schemaManager.getAis().getUserTable(rowDef.getSchemaName(),
    // rowDef.getTableName());
    // if (uTable != null) {
    // Column autoIncColumn = uTable.getAutoIncrementColumn();
    // if (autoIncColumn != null) {
    // Long autoIncValue = autoIncColumn.getInitialAutoIncrementValue();
    // if (autoIncValue != null) {
    // value = autoIncValue;
    // }
    // }
    // }
    // }
    // releaseExchange(exchange);
    // return value;
    // }

    @Override
    public RowCollector getSavedRowCollector(final Session session,
            final int tableId) throws InvalidOperationException {
        final List<RowCollector> list = collectorsForTableId(session, tableId);
        if (list.isEmpty()) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Nested RowCollector on tableId=" + tableId
                        + " depth=" + (list.size() + 1));
            }
            throw new InvalidOperationException(ErrorCode.CURSOR_IS_FINISHED,
                    "No RowCollector for tableId=%d (depth=%d)", tableId,
                    list.size() + 1);
        }
        return list.get(list.size() - 1);
    }

    @Override
    public void addSavedRowCollector(final Session session,
            final RowCollector rc) {
        final Integer tableId = rc.getTableId();
        final List<RowCollector> list = collectorsForTableId(session, tableId);
        if (!list.isEmpty()) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Note: Nested RowCollector on tableId=" + tableId
                        + " depth=" + (list.size() + 1));
            }
            assert list.get(list.size() - 1) != rc : "Redundant call";
            //
            // This disallows the patch because we agreed not to fix the
            // bug. However, these changes fix a memory leak, which is
            // important for robustness.
            //
            // throw new StoreException(122, "Bug 255 workaround is disabled");
        }
        list.add(rc);
    }

    @Override
    public void removeSavedRowCollector(final Session session,
            final RowCollector rc) throws InvalidOperationException {
        final Integer tableId = rc.getTableId();
        final List<RowCollector> list = collectorsForTableId(session, tableId);
        if (list.isEmpty()) {
            throw new InvalidOperationException(ErrorCode.INTERNAL_ERROR,
                    "Attempt to remove RowCollector from empty list");
        }
        final RowCollector removed = list.remove(list.size() - 1);
        if (removed != rc) {
            throw new InvalidOperationException(ErrorCode.INTERNAL_ERROR,
                    "Attempt to remove the wrong RowCollector");
        }
    }

    private List<RowCollector> collectorsForTableId(final Session session,
            final int tableId) {
        Map<Integer, List<RowCollector>> map = session.get("store",
                "collectors");
        if (map == null) {
            map = new HashMap<Integer, List<RowCollector>>();
            session.put("store", "collectors", map);
        }
        List<RowCollector> list = map.get(tableId);
        if (list == null) {
            list = new ArrayList<RowCollector>();
            map.put(tableId, list);
        }
        return list;
    }

    public final RowDef checkRequest(int rowDefId, RowData start, RowData end,
            int indexId, int scanFlags) throws InvalidOperationException,
            PersistitException {
        final TableStatus ts = checkTableStatus(rowDefId);

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

    public RowCollector newRowCollector(final Session session,
            ScanRowsRequest request) throws InvalidOperationException,
            PersistitException {
        NEW_COLLECTOR_TAP.in();

        int rowDefId = request.getTableId();
        RowData start = request.getStart();
        RowData end = request.getEnd();
        int indexId = request.getIndexId();
        int scanFlags = request.getScanFlags();
        byte[] columnBitMap = request.getColumnBitMap();
        final RowDef rowDef = checkRequest(rowDefId, start, end, indexId,
                scanFlags);
        RowCollector rc = new PersistitStoreRowCollector(session, this,
                scanFlags, start, end, columnBitMap, rowDef, indexId);
        NEW_COLLECTOR_TAP.out();
        return rc;
    }

    @Override
    public RowCollector newRowCollector(final Session session,
            final int rowDefId, int indexId, final int scanFlags,
            RowData start, RowData end, byte[] columnBitMap)
            throws InvalidOperationException, PersistitException {

        NEW_COLLECTOR_TAP.in();
        final RowDef rowDef = checkRequest(rowDefId, start, end, indexId,
                scanFlags);

        final RowCollector rc = new PersistitStoreRowCollector(session, this,
                scanFlags, start, end, columnBitMap, rowDef, indexId);

        NEW_COLLECTOR_TAP.out();
        return rc;
    }

    public final static long HACKED_ROW_COUNT = 2;

    @Override
    public long getRowCount(final Session session, final boolean exact,
            final RowData start, final RowData end, final byte[] columnBitMap)
            throws Exception {
        //
        // TODO: Compute a reasonable value. The value "2" is a hack -
        // special because it's not 0 or 1, but small enough to induce
        // MySQL to use an index rather than full table scan.
        //
        return HACKED_ROW_COUNT; // TODO: delete the HACKED_ROW_COUNT field when
                                 // this gets fixed
        // final int tableId = start.getRowDefId();
        // final TableStatus status = tableManager.getTableStatus(tableId);
        // return status.getRowCount();
    }

    @Override
    public TableStatistics getTableStatistics(final Session session, int tableId)
            throws Exception {
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
        ts.setUpdateTime(Math.max(status.getLastUpdateTime(),
                status.getLastWriteTime()));
        ts.setCreationTime(status.getCreationTime());
        // TODO - get correct values
        ts.setMeanRecordLength(100);
        ts.setBlockSize(8192);
        indexManager.populateTableStatistics(session, ts);
        return ts;
    }

    @Override
    public void analyzeTable(final Session session, final int tableId)
            throws Exception {
        final RowDef rowDef = rowDefCache.getRowDef(tableId);
        indexManager.analyzeTable(session, rowDef);
    }

    // FOR TESTING ONLY
    @Override
    public List<RowData> fetchRows(final Session session,
            final String schemaName, final String tableName,
            final String columnName, final Object least, final Object greatest,
            final String leafTableName) throws Exception {
        final ByteBuffer payload = ByteBuffer.allocate(65536);
        return fetchRows(session, schemaName, tableName, columnName, least,
                greatest, leafTableName, payload);
    }

    public List<RowData> fetchRows(final Session session,
            final String schemaName, final String tableName,
            final String columnName, final Object least, final Object greatest,
            final String leafTableName, final ByteBuffer payload)
            throws Exception {
        final List<RowData> list = new ArrayList<RowData>();
        final String compoundName = schemaName + "." + tableName;
        final RowDef rowDef = rowDefCache.getRowDef(compoundName);
        if (rowDef == null) {
            throw new InvalidOperationException(ErrorCode.NO_SUCH_TABLE,
                    compoundName);
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
            throw new InvalidOperationException(ErrorCode.NO_SUCH_COLUMN,
                    columnName + " in " + compoundName);
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
            throw new InvalidOperationException(ErrorCode.NO_INDEX,
                    "on column " + columnName + " in " + compoundName);
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
            throw new InvalidOperationException(ErrorCode.NO_SUCH_TABLE,
                    leafTableName + " in group");
        }

        RowData start = null;
        RowData end = null;

        int flags = deepMode ? SCAN_FLAGS_DEEP : 0;
        if (least == null) {
            flags |= SCAN_FLAGS_START_AT_EDGE;
        } else {
            final Object[] startValues = new Object[groupRowDef.getFieldCount()];
            startValues[fieldDef.getFieldIndex() + rowDef.getColumnOffset()] = least;
            start = new RowData(new byte[1024]);
            start.createRow(groupRowDef, startValues);
        }

        if (greatest == null) {
            flags |= SCAN_FLAGS_END_AT_EDGE;
        } else {
            final Object[] endValues = new Object[groupRowDef.getFieldCount()];
            endValues[fieldDef.getFieldIndex() + rowDef.getColumnOffset()] = greatest;
            end = new RowData(new byte[1024]);
            end.createRow(groupRowDef, endValues);
        }

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

        final RowCollector rc = newRowCollector(session,
                groupRowDef.getRowDefId(), indexDef.getId(), flags, start, end,
                bitMap);
        while (rc.hasMore()) {
            payload.clear();
            while (rc.collectNextRow(payload))
                ;
            payload.flip();
            for (int p = payload.position(); p < payload.limit();) {
                final RowData rowData = new RowData(payload.array(), p,
                        payload.limit());
                rowData.prepareRow(p);
                list.add(rowData);
                p = rowData.getRowEnd();
            }
        }
        rc.close();
        return list;
    }

    // ---------------------------------
    void insertIntoIndex(final Session session, final IndexDef indexDef,
            final RowData rowData, final Key hkey, final boolean deferIndexes)
            throws InvalidOperationException, PersistitException {
        final Exchange iEx = getExchange(session, indexDef.getRowDef(),
                indexDef);
        constructIndexKey(iEx.getKey(), rowData, indexDef, hkey);
        final Key key = iEx.getKey();

        if (indexDef.isUnique()) {
            int saveSize = key.getEncodedSize();
            key.setDepth(indexDef.getIndexKeySegmentCount());
            if (iEx.hasChildren()) {
                throw new InvalidOperationException(ErrorCode.DUPLICATE_KEY,
                        "Non-unique index key: %s", key);
            }
            key.setEncodedSize(saveSize);
        }
        iEx.getValue().clear();
        if (deferIndexes) {
            synchronized (deferredIndexKeys) {
                SortedSet<KeyState> keySet = deferredIndexKeys.get(iEx.getTree());
                if (keySet == null) {
                    keySet = new TreeSet<KeyState>();
                    deferredIndexKeys.put(iEx.getTree(), keySet);
                }
                final KeyState ks = new KeyState(iEx.getKey());
                keySet.add(ks);
                deferredIndexKeyLimit -= (ks.getBytes().length + KEY_STATE_SIZE_OVERHEAD);
            }
        } else {
            iEx.store();
        }
        releaseExchange(session, iEx);
    }

    void putAllDeferredIndexKeys(final Session session)
            throws PersistitException {
        synchronized (deferredIndexKeys) {
            for (final Map.Entry<Tree, SortedSet<KeyState>> entry : deferredIndexKeys
                    .entrySet()) {
                final Exchange iEx = getExchange(session, entry.getKey());
                buildIndexAddKeys(entry.getValue(), iEx);
                entry.getValue().clear();
            }
            deferredIndexKeyLimit = MAX_INDEX_TRANCHE_SIZE;
        }
    }

    void updateIndex(final Session session, final IndexDef indexDef,
            final RowDef rowDef, final RowData oldRowData,
            final RowData newRowData, final Key hkey) throws PersistitException {

        if (!fieldsEqual(rowDef, oldRowData, newRowData, indexDef.getFields())) {
            final Exchange oldExchange = getExchange(session, rowDef, indexDef);
            constructIndexKey(oldExchange.getKey(), oldRowData, indexDef, hkey);
            final Exchange newExchange = getExchange(session, rowDef, indexDef);
            constructIndexKey(newExchange.getKey(), newRowData, indexDef, hkey);

            oldExchange.getValue().clear();
            newExchange.getValue().clear();

            oldExchange.remove();
            newExchange.store();

            releaseExchange(session, newExchange);
            releaseExchange(session, oldExchange);
        }
    }

    void deleteIndex(final Session session, final IndexDef indexDef,
            final RowDef rowDef, final RowData rowData, final Key hkey)
            throws PersistitException {
        final Exchange iEx = getExchange(session, rowDef, indexDef);
        constructIndexKey(iEx.getKey(), rowData, indexDef, hkey);
        iEx.remove();
        releaseExchange(session, iEx);
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
            if (!bytesEqual(a.getBytes(), (int) aloc, (int) (aloc >>> 32),
                    b.getBytes(), (int) bloc, (int) (bloc >>> 32))) {
                return false;
            }
        }
        return true;
    }

    public void packRowData(final Exchange hEx, final RowDef rowDef,
            final RowData rowData) {
        final int start = rowData.getInnerStart();
        final int size = rowData.getInnerSize();
        hEx.getValue().ensureFit(size);
        System.arraycopy(rowData.getBytes(), start, hEx.getValue()
                .getEncodedBytes(), 0, size);
        hEx.getValue().setEncodedSize(size);
    }

    public void expandRowData(final Exchange exchange, final RowDef rowDef,
            final RowData rowData) throws InvalidOperationException { // TODO
                                                                      // this
                                                                      // needs
                                                                      // to be a
                                                                      // more
                                                                      // specific
                                                                      // exception
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
            throw new InvalidOperationException(ErrorCode.INTERNAL_CORRUPTION,
                    "Corrupt RowData at " + exchange.getKey());
        }

        final int rowDefId = CServerUtil.getInt(valueBytes,
                RowData.O_ROW_DEF_ID - RowData.LEFT_ENVELOPE_SIZE);
        if (rowDef != null) {
            final int expectedRowDefId = rowDef.getRowDefId();
            if (rowDefId != expectedRowDefId) {
                //
                // TODO: Add code to here to evolve data to required
                // expectedRowDefId
                //
                throw new InvalidOperationException(
                        ErrorCode.MULTIGENERATIONAL_TABLE,
                        "Unable to convert rowDefId " + rowDefId
                                + " to expected rowDefId " + expectedRowDefId);
            }
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

    public void buildIndexes(final Session session, final String ddl) {
        flushIndexes(session);

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
                        session, rowDef.getRowDefId(), 0, 0, rowData, rowData,
                        columnBitMap);
                // final KeyFilter hFilter = rc.getHFilter();
                final Exchange hEx = rc.getHExchange();

                hEx.getKey().clear();
                // while (hEx.traverse(Key.GT, hFilter, Integer.MAX_VALUE)) {
                while (hEx.next(true)) {
                    expandRowData(hEx, null, rowData);
                    final int tableId = rowData.getRowDefId();
                    final RowDef userRowDef = rowDefCache.getRowDef(tableId);
                    if (userRowDefs.contains(userRowDef)) {
                        for (final IndexDef indexDef : userRowDef
                                .getIndexDefs()) {
                            if (isIndexSelected(indexDef, ddl)) {
                                insertIntoIndex(session, indexDef, rowData,
                                        hEx.getKey(), true);
                                indexKeyCount++;
                            }
                        }
                        if (deferredIndexKeyLimit <= 0) {
                            putAllDeferredIndexKeys(session);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Exception while trying to index group table "
                        + rowDef.getSchemaName() + "." + rowDef.getTableName(),
                        e);
            }
            flushIndexes(session);
            if (LOG.isInfoEnabled()) {
                LOG.info("Inserted " + indexKeyCount
                        + " index keys into group " + rowDef.getSchemaName()
                        + "." + rowDef.getTableName());
            }
        }
    }

    public void flushIndexes(final Session session) {
        try {
            putAllDeferredIndexKeys(session);
        } catch (Exception e) {
            LOG.error("Exception while trying "
                    + " to flush deferred index keys", e);
        }
    }

    public void deleteIndexes(final Session session, final String ddl) {
        for (final RowDef rowDef : rowDefCache.getRowDefs()) {
            if (!rowDef.isGroupTable()) {
                for (final IndexDef indexDef : rowDef.getIndexDefs()) {
                    if (isIndexSelected(indexDef, ddl)) {
                        try {
                            final Exchange iEx = getExchange(session, rowDef,
                                    indexDef);
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
                && (!ddl.contains("table=") || ddl.contains("table=("
                        + indexDef.getRowDef().getTableName() + ")"))
                && (!ddl.contains("index=") || ddl.contains("index=("
                        + indexDef.getName() + ")"));
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

    @Override
    public boolean isDeferIndexes() {
        return deferIndexes;
    }

    @Override
    public void setDeferIndexes(final boolean defer) {
        deferIndexes = defer;
    }
}
