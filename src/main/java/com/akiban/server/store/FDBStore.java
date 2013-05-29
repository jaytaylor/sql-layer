/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.akiban.server.store;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.HKeyColumn;
import com.akiban.ais.model.HKeySegment;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexToHKey;
import com.akiban.ais.model.NopVisitor;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.persistitadapter.FDBAdapter;
import com.akiban.qp.persistitadapter.FDBGroupCursor;
import com.akiban.qp.persistitadapter.FDBGroupRow;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRowBuffer;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.TableStatistics;
import com.akiban.server.TableStatus;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.error.DuplicateKeyException;
import com.akiban.server.error.NoSuchRowException;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.IndexDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.KeyCreator;
import com.akiban.server.service.tree.TreeLink;
import com.akiban.server.store.statistics.IndexStatisticsService;
import com.foundationdb.KeyValue;
import com.foundationdb.RangeQuery;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import com.google.inject.Inject;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Value;
import com.persistit.exception.PersistitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FDBStore extends AbstractStore implements KeyCreator, Service {
    private static final Logger LOG = LoggerFactory.getLogger(FDBStore.class.getName());

    private final ConfigurationService configService;
    private final SchemaManager schemaManager;
    private final FDBTransactionService txnService;

    @Inject
    public FDBStore(ConfigurationService configService,
                    SchemaManager schemaManager,
                    TransactionService txnService) {
        this.configService = configService;
        this.schemaManager = schemaManager;
        if(txnService instanceof FDBTransactionService) {
            this.txnService = (FDBTransactionService)txnService;
        } else {
            throw new IllegalStateException("Only usable with FDBTransactionService, found: " + txnService);
        }
    }

    public Iterator<KeyValue> groupIterator(Session session, Group group) {
        Transaction txn = txnService.getTransaction(session);
        byte[] packedPrefix = packedTuple(group);
        //print("Group scan: ", packedPrefix);
        return txn.getRangeStartsWith(packedPrefix).iterator();
    }

    // TODO: Creates range for hKey and descendents, add another API to specify
    public Iterator<KeyValue> groupIterator(Session session, Group group, Key hKey) {
        Transaction txn = txnService.getTransaction(session);
        byte[] packedPrefix = packedTuple(group, hKey);
        Key after = createKey();
        hKey.copyTo(after);
        after.append(Key.AFTER);
        byte[] packedAfter = packedTuple(group, after);
        //print("Group scan: [", packedPrefix, ",", packedAfter, ")");
        return txn.getRange(packedPrefix, packedAfter).iterator();
    }

    public Iterator<KeyValue> indexIterator(Session session, Index index, boolean reverse) {
        Transaction txn = txnService.getTransaction(session);
        byte[] packedPrefix = packedTuple(index);
        //print("Index scan: ", packedPrefix, "reverse: ", reverse);
        RangeQuery range = txn.getRangeStartsWith(packedPrefix);
        if(reverse) {
            range = range.reverse();
        }
        return range.iterator();
    }

    public Iterator<KeyValue> indexIterator(Session session, Index index, Key start, Key end, boolean reverse) {
        Transaction txn = txnService.getTransaction(session);
        byte[] packedStart = packedTuple(index, start);
        byte[] packedEnd = packedTuple(index, end);
        //print("Index scan: [", packedStart, ",", packedEnd, ")", " reverse:", reverse);
        RangeQuery range = txn.getRange(packedStart, packedEnd);
        if(reverse) {
            range = range.reverse();
        }
        return range.iterator();
    }

    public long nextSequenceValue(Session session, Sequence sequence) {
        Transaction txn = txnService.getTransaction(session);
        byte[] packedTuple = packedTuple(sequence);
        long rawValue = 0;
        byte[] byteValue = txn.get(packedTuple).get();
        if(byteValue != null) {
            Tuple tuple = Tuple.fromBytes(byteValue);
            rawValue = tuple.getLong(0);
        }
        rawValue += 1;
        long outValue = sequence.nextValueRaw(rawValue);
        txn.set(packedTuple, Tuple.from(rawValue).pack());
        return outValue;
    }

    public long curSequenceValue(Session session, Sequence sequence) {
        Transaction txn = txnService.getTransaction(session);
        long rawValue = 0;
        byte[] byteValue = txn.get(packedTuple(sequence)).get();
        if(byteValue != null) {
            Tuple tuple = Tuple.fromBytes(byteValue);
            rawValue = tuple.getLong(0);
        }
        return sequence.currentValueRaw(rawValue);
    }


    //
    // Service
    //

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void crash() {
    }


    //
    // Store
    //

    @Override
    public AkibanInformationSchema getAIS(Session session) {
        return schemaManager.getAis(session);
    }

    @Override
    public void writeRow(Session session, RowData rowData) {
        final RowDef rowDef = rowDefFromExplicitOrId(session, rowData);
        Transaction txn = txnService.getTransaction(session);

        Key hKey = createKey();
        constructHKey(txn, hKey, rowDef, rowData, true);

        byte[] packedKey = packedTuple(rowDef.getGroup(), hKey);
        byte[] packedValue = Arrays.copyOfRange(rowData.getBytes(), rowData.getBufferStart(), rowData.getBufferEnd());

        // store
        txn.set(packedKey, packedValue);

        if(rowDef.isAutoIncrement()) {
            LOG.error("mysql auto increment RowDef: {}", rowDef);
        }

        //PersistitIndexRowBuffer indexRow = new PersistitIndexRowBuffer(adapter(session));
        PersistitIndexRowBuffer indexRow = new PersistitIndexRowBuffer(this);
        Key indexKey = createKey();
        for(Index index : rowDef.getIndexes()) {
            insertIntoIndex(txn, index, rowData, hKey, indexKey, indexRow);
        }

        // bug1112940: Bump row count *after* uniqueness checks in insertIntoIndex
        rowDef.getTableStatus().rowsWritten(1);

        if (/*propagateHKeyChanges &&*/ rowDef.userTable().hasChildren()) {
            LOG.warn("propagateHKeyChanges skipped: {}", rowDef);
            /*
            // The row being inserted might be the parent of orphan rows
            // already present. The hkeys of these
            // orphan rows need to be maintained. The hkeys of interest
            // contain the PK from the inserted row,
            // and nulls for other hkey fields nearer the root.
            // TODO: optimizations
            // - If we knew that no descendent table had an orphan (e.g.
            // store this info in TableStatus),
            // then this propagation could be skipped.
            hEx.clear();
            Key hKey = hEx.getKey();
            PersistitKeyAppender hKeyAppender = PersistitKeyAppender.create(hKey);
            UserTable table = rowDef.userTable();
            List<Column> pkColumns = table.getPrimaryKeyIncludingInternal().getColumns();
            List<HKeySegment> hKeySegments = table.hKey().segments();
            int s = 0;
            while (s < hKeySegments.size()) {
                HKeySegment segment = hKeySegments.get(s++);
                RowDef segmentRowDef = segment.table().rowDef();
                hKey.append(segmentRowDef.getOrdinal());
                List<HKeyColumn> hKeyColumns = segment.columns();
                int c = 0;
                while (c < hKeyColumns.size()) {
                    HKeyColumn hKeyColumn = hKeyColumns.get(c++);
                    Column column = hKeyColumn.column();
                    RowDef columnTableRowDef = column.getTable().rowDef();
                    if (pkColumns.contains(column)) {
                        hKeyAppender.append(columnTableRowDef.getFieldDef(column.getPosition()), rowData);
                    } else {
                        hKey.append(null);
                    }
                }
            }
            propagateDownGroup(session, hEx, tablesRequiringHKeyMaintenance, indexRow, true, false);
            */
        }
    }

    @Override
    public void deleteRow(Session session,
                          RowData rowData,
                          boolean deleteIndexes,
                          boolean cascadeDelete) {
        final RowDef rowDef = rowDefFromExplicitOrId(session, rowData);
        Transaction txn = txnService.getTransaction(session);
        Key hKey = createKey();
        constructHKey(txn, hKey, rowDef, rowData, false);

        byte[] packedKey = packedTuple(rowDef.getGroup(), hKey);
        byte[] fetched = txn.get(packedKey).get();
        if(fetched == null) {
            throw new NoSuchRowException(hKey);
        }

        // record the deletion of the old index row
        //if (deleteIndexes)
        //    addChangeFor(rowDef.userTable(), session, hEx.getKey());

        // Remove the h-row
        txn.clear(packedKey);
        rowDef.getTableStatus().rowDeleted();

        // Remove the indexes, including the PK index
        PersistitIndexRowBuffer indexRow = new PersistitIndexRowBuffer(this);
        if(deleteIndexes) {
            for (Index index : rowDef.getIndexes()) {
                deleteIndex(txn, index, rowData, hKey, indexRow);
            }
        }

        // The row being deleted might be the parent of rows that
        // now become orphans. The hkeys
        // of these rows need to be maintained.
        if(/*propagateHKeyChanges && */rowDef.userTable().hasChildren()) {
            LOG.warn("propagateHKeyChanges skipped: {}", rowDef);
            //propagateDownGroup(session, hEx, tablesRequiringHKeyMaintenance, indexRow, deleteIndexes, cascadeDelete);
        }
    }

    @Override
    public void updateRow(Session session,
                          RowData oldRowData,
                          RowData newRowData,
                          ColumnSelector columnSelector,
                          Index[] indexes) {
        if(columnSelector != null) {
            final RowDef rowDef = rowDefFromExplicitOrId(session, oldRowData);
            for(int i = 0; i < rowDef.getFieldCount(); ++i) {
                if(!columnSelector.includesColumn(i)) {
                    throw new UnsupportedOperationException("ALL COLUMN selector required");
                }
            }
        }
        deleteRow(session, oldRowData, true, false);
        writeRow(session, newRowData);
    }

    @Override
    public void truncateTree(Session session, TreeLink treeLink) {
        Transaction txn = txnService.getTransaction(session);
        txn.clearRangeStartsWith(packedTuple(treeLink));
    }

    @Override
    public PersistitStore getPersistitStore() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void buildIndexes(Session session, Collection<? extends Index> indexes, boolean deferIndexes) {
        Set<Group> groups = new HashSet<>();
        Map<Integer,RowDef> userRowDefs = new HashMap<>();
        Set<Index> indexesToBuild = new HashSet<>();
        for(Index index : indexes) {
            IndexDef indexDef = index.indexDef();
            if(indexDef == null) {
                throw new IllegalArgumentException("indexDef was null for index: " + index);
            }
            indexesToBuild.add(index);
            RowDef rowDef = indexDef.getRowDef();
            userRowDefs.put(rowDef.getRowDefId(), rowDef);
            groups.add(rowDef.table().getGroup());
        }

        Transaction txn = txnService.getTransaction(session);
        FDBAdapter adapter = createAdapter(session, SchemaCache.globalSchema(getAIS(session)));
        Key indexKey = createKey();
        PersistitIndexRowBuffer indexRowBuffer = new PersistitIndexRowBuffer(this);

        for(Group group : groups) {
            FDBGroupCursor cursor = adapter.newGroupCursor(group);
            cursor.open();
            FDBGroupRow row;
            while((row = cursor.next()) != null) {
                RowData rowData = row.rowData();
                int tableId = rowData.getRowDefId();
                RowDef userRowDef = userRowDefs.get(tableId);
                if(userRowDef != null) {
                    for(Index index : userRowDef.getIndexes()) {
                        if(indexesToBuild.contains(index)) {
                            insertIntoIndex(txn, index, rowData, row.hKey().key(), indexKey, indexRowBuffer);
                        }
                    }
                }
            }
            cursor.close();
            cursor.destroy();
        }
    }

    @Override
    public void removeTree(Session session, TreeLink treeLink) {
        if(!schemaManager.treeRemovalIsDelayed()) {
            truncateTree(session, treeLink);
        }
        schemaManager.treeWasRemoved(session, treeLink.getSchemaName(), treeLink.getTreeName());
    }

    @Override
    public void truncateIndexes(Session session, Collection<? extends Index> indexes) {
        super.truncateIndexes(session, indexes);
        // TODO: GI row counts
    }

    @Override
    public void deleteSequences(Session session, Collection<? extends Sequence> sequences) {
        removeTrees(session, sequences);
    }

    @Override
    public void startBulkLoad(Session session) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishBulkLoad(Session session) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBulkloading() {
        return false;
    }

    @Override
    public FDBAdapter createAdapter(Session session, Schema schema) {
        return new FDBAdapter(this, schema, session, configService);
    }


    //
    // KeyCreator
    //

    @Override
    public Key createKey() {
        // TODO: null probably won't work for collated strings, needs persistit for class storage
        return new Key(null, 2047);
    }


    //
    // Internal
    //

    // TODO: Copied from PersistitStore, consolidate
    private long constructHKey(Transaction txn, Key hKey, RowDef rowDef, RowData rowData, boolean insertingRow) {
        // Initialize the hkey being constructed
        long uniqueId = -1;
        PersistitKeyAppender hKeyAppender = PersistitKeyAppender.create(hKey);
        hKeyAppender.key().clear();
        // Metadata for the row's table
        UserTable table = rowDef.userTable();
        FieldDef[] fieldDefs = rowDef.getFieldDefs();

        // Only set if parent row is looked up
        Key parentPKKey = null;
        TableIndex parentPKIndex = null;
        RowDef parentRowDef = null;
        IndexToHKey indexToHKey = null;
        PersistitIndexRowBuffer parentPKIndexRow = null;
        int i2hPosition = 0;

        // Nested loop over hkey metadata: All the segments of an hkey, and all
        // the columns of a segment.
        List<HKeySegment> hKeySegments = table.hKey().segments();
        int s = 0;
        while (s < hKeySegments.size()) {
            HKeySegment hKeySegment = hKeySegments.get(s++);
            // Write the ordinal for this segment
            RowDef segmentRowDef = hKeySegment.table().rowDef();
            hKeyAppender.append(segmentRowDef.table().getOrdinal());
            // Iterate over the segment's columns
            List<HKeyColumn> hKeyColumns = hKeySegment.columns();
            int c = 0;
            while (c < hKeyColumns.size()) {
                HKeyColumn hKeyColumn = hKeyColumns.get(c++);
                UserTable hKeyColumnTable = hKeyColumn.column().getUserTable();
                if (hKeyColumnTable != table) {
                    // Hkey column from row of parent table
                    if (parentPKKey == null) {
                        // Initialize parent metadata and state
                        parentPKKey = createKey();
                        parentRowDef = rowDef.getParentRowDef();
                        parentPKIndex = parentRowDef.getPKIndex();
                        indexToHKey = parentPKIndex.indexToHKey();
                        parentPKIndexRow = readPKIndexRow(txn, parentPKIndex, parentPKKey, rowDef, rowData);
                    }
                    if(indexToHKey.isOrdinal(i2hPosition)) {
                        assert indexToHKey.getOrdinal(i2hPosition) == segmentRowDef.table().getOrdinal() : hKeyColumn;
                        ++i2hPosition;
                    }
                    if (parentPKIndexRow != null) {
                        parentPKIndexRow.appendFieldTo(indexToHKey.getIndexRowPosition(i2hPosition), hKeyAppender.key());
                    } else {
                        hKeyAppender.appendNull(); // orphan row
                    }
                    ++i2hPosition;
                } else {
                    // Hkey column from rowData
                    Column column = hKeyColumn.column();
                    FieldDef fieldDef = fieldDefs[column.getPosition()];
                    if (insertingRow && column.isAkibanPKColumn()) {
                        // Must be a PK-less table. Use unique id from TableStatus.
                        uniqueId = segmentRowDef.getTableStatus().createNewUniqueID();
                        hKeyAppender.append(uniqueId);
                        // Write rowId into the value part of the row also.
                        rowData.updateNonNullLong(fieldDef, uniqueId);
                    } else {
                        hKeyAppender.append(fieldDef, rowData);
                    }
                }
            }
        }
        return uniqueId;
    }

    private void insertIntoIndex(Transaction txn,
                                 Index index,
                                 RowData rowData,
                                 Key hKey,
                                 Key indexKey,
                                 PersistitIndexRowBuffer indexRow)
    {
        constructIndexRow(indexKey, rowData, index, hKey, indexRow, true);
        checkUniqueness(txn, index, rowData, indexKey);

        txn.set(packedTuple(index, indexRow.getPKey()),
                Arrays.copyOf(indexRow.getPValue().getEncodedBytes(), indexRow.getPValue().getEncodedSize()));
    }

    private static void constructIndexRow(Key indexKey,
                                          RowData rowData,
                                          Index index,
                                          Key hKey,
                                          PersistitIndexRowBuffer indexRow,
                                          boolean forInsert) {
        indexKey.clear();
        indexRow.resetForWrite(index, indexKey, new Value((Persistit)null));
        indexRow.initialize(rowData, hKey);
        indexRow.close(forInsert);
    }

    private void checkUniqueness(Transaction txn, Index index, RowData rowData, Key key) {
        if(index.isUnique() && !hasNullIndexSegments(rowData, index)) {
            int segmentCount = index.indexDef().getIndexKeySegmentCount();
            // An index that isUniqueAndMayContainNulls has the extra null-separating field.
            if (index.isUniqueAndMayContainNulls()) {
                segmentCount++;
            }
            key.setDepth(segmentCount);
            if(keyExistsInIndex(txn, index, key)) {
                throw new DuplicateKeyException(index.getIndexName().getName(), key);
            }
        }
    }

    private boolean keyExistsInIndex(Transaction txn, Index index, Key key) {
        assert index.isUnique() : index;
        return txn.getRangeStartsWith(packedTuple(index, key)).iterator().hasNext();
    }

    private byte[] packedTuple(Index index) {
        return packedTuple(index.indexDef());
    }

    private byte[] packedTuple(Index index, Key key) {
        return packedTuple(index.indexDef(), key);
    }

    private byte[] packedTuple(TreeLink treeLink) {
        return Tuple.from(treeLink.getTreeName(), "/").pack();
    }

    private byte[] packedTuple(TreeLink treeLink, Key key) {
        byte[] keyBytes = Arrays.copyOf(key.getEncodedBytes(), key.getEncodedSize());
        return Tuple.from(treeLink.getTreeName(), "/", keyBytes).pack();
    }

    private PersistitIndexRowBuffer readPKIndexRow(Transaction txn,
                                                   Index parentPkIndex,
                                                   Key parentPkKey,
                                                   RowDef childRowDef,
                                                   RowData childRowData)
    {
        PersistitKeyAppender keyAppender = PersistitKeyAppender.create(parentPkKey);
        int[] fields = childRowDef.getParentJoinFields();
        for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
            FieldDef fieldDef = childRowDef.getFieldDef(fields[fieldIndex]);
            keyAppender.append(fieldDef, childRowData);
        }

        byte[] pkValue = txn.get(packedTuple(parentPkIndex, parentPkKey)).get();
        PersistitIndexRowBuffer indexRow = null;
        if (pkValue != null) {
            Value value = new Value((Persistit)null);
            value.putByteArray(pkValue);
            indexRow = new PersistitIndexRowBuffer(this);
            indexRow.resetForRead(parentPkIndex, parentPkKey, value);
        }
        return indexRow;
    }

    private void deleteIndex(Transaction txn,
                             Index index,
                             RowData rowData,
                             Key hkey,
                             PersistitIndexRowBuffer indexRowBuffer) {
        deleteIndexRow(txn, index, createKey(), rowData, hkey, indexRowBuffer);
    }

    private void deleteIndexRow(Transaction txn,
                                Index index,
                                Key indexKey,
                                RowData rowData,
                                Key hKey,
                                PersistitIndexRowBuffer indexRowBuffer) {
        if (index.isUniqueAndMayContainNulls()) {
            // TODO: Is PersistitStore's broken w.r.t indexRow.hKey()?
            throw new UnsupportedOperationException("Can't delete unique index with nulls");
        } else {
            constructIndexRow(indexKey, rowData, index, hKey, indexRowBuffer, false);
            txn.clear(packedTuple(index, indexKey));
        }
    }

    private static void print(Object... objs) {
        for(Object o : objs) {
            if(o instanceof byte[]) {
                byte[] packed = (byte[])o;
                System.out.print("'");
                for(byte b : packed) {
                    int c = 0xFF & b;
                    if(c <= 32 || c >= 127) {
                        System.out.printf("\\x%02d", c);
                    } else {
                        System.out.printf("%c", (char)c);
                    }
                }
                System.out.print("'");
            } else{
                System.out.print(o);
            }
        }
        System.out.println();
    }
}
