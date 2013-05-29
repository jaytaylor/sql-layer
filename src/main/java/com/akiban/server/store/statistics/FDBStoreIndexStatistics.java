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

package com.akiban.server.store.statistics;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.server.rowdata.IndexDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.FDBStore;
import com.foundationdb.KeyValue;
import com.foundationdb.tuple.Tuple;
import com.persistit.Key;

import java.util.Iterator;

import static com.akiban.server.store.statistics.IndexStatisticsVisitor.VisitorCreator;

public class FDBStoreIndexStatistics extends AbstractStoreIndexStatistics<FDBStore> implements VisitorCreator<Key,byte[]> {
    private final IndexStatisticsService indexStatisticsService;

    public FDBStoreIndexStatistics(FDBStore store, IndexStatisticsService indexStatisticsService) {
        super(store);
        this.indexStatisticsService = indexStatisticsService;
    }


    //
    // AbstractStoreIndexStatistics
    //

    @Override
    public IndexStatistics loadIndexStatistics(Session session, Index index) {
        IndexDef indexDef = index.indexDef();
        RowDef indexStatisticsRowDef = getIndexStatsRowDef(session);
        RowDef indexStatisticsEntryRowDef = getIndexStatsEntryRowDef(session);

        Key hKey = getStore().createKey();
        hKey.append(indexStatisticsRowDef.table().getOrdinal())
            .append((long) indexDef.getRowDef().getRowDefId())
            .append((long) index.getIndexId());

        IndexStatistics result = null;
        Iterator<KeyValue> it = getStore().groupIterator(session, indexStatisticsRowDef.getGroup(), hKey);
        while(it.hasNext()) {
            KeyValue kv = it.next();
            if(result == null) {
                result = decodeHeader(kv, indexStatisticsRowDef, index);
            } else {
                decodeEntry(kv, indexStatisticsEntryRowDef, result);
            }
        }
        return result;
    }

    @Override
    public void removeStatistics(Session session, Index index) {
        IndexDef indexDef = index.indexDef();
        RowDef indexStatisticsRowDef = getIndexStatsRowDef(session);

        if(indexDef == null) {
            return;
        }

        Key hKey = getStore().createKey();
        hKey.append(indexStatisticsRowDef.table().getOrdinal())
                .append((long) indexDef.getRowDef().getRowDefId())
                .append((long) index.getIndexId());

        Iterator<KeyValue> it = getStore().groupIterator(session, indexStatisticsRowDef.getGroup(), hKey);
        while(it.hasNext()) {
            KeyValue kv = it.next();
            RowData rowData = new RowData();
            rowData.reset(kv.getValue());
            rowData.prepareRow(0);
            getStore().deleteRow(session, rowData, true, false); // TODO: Use cascade?
        }
    }

    @Override
    public IndexStatistics computeIndexStatistics(Session session, Index index) {
        long indexRowCount = indexStatisticsService.countEntries(session, index);
        IndexStatisticsVisitor<Key,byte[]> visitor = new IndexStatisticsVisitor<>(session, index, indexRowCount, this);
        int bucketCount = indexStatisticsService.bucketCount();
        visitor.init(bucketCount);
        Iterator<KeyValue> it = getStore().indexIterator(session, index, false);
        while(it.hasNext()) {
            KeyValue kv = it.next();
            // TODO: Consolidate KeyValue -> (Key,[byte[]|RowData])
            byte[] keyBytes = Tuple.fromBytes(kv.getKey()).getBytes(2);
            Key key = getStore().createKey();
            System.arraycopy(keyBytes, 0, key.getEncodedBytes(), 0, keyBytes.length);
            key.setEncodedSize(keyBytes.length);
            visitor.visit(key, kv.getValue());
        }
        visitor.finish(bucketCount);
        return visitor.getIndexStatistics();
    }

    @Override
    public long manuallyCountEntries(Session session, Index index) {
        int count = 0;
        Iterator<KeyValue> it = getStore().indexIterator(session, index, false);
        while(it.hasNext()) {
            it.next();
            ++count;
        }
        return count;
    }


    //
    // VisitorCreator
    //

    @Override
    public IndexStatisticsGenerator<Key,byte[]> multiColumnVisitor(Index index) {
        return new FDBMultiColumnIndexStatisticsVisitor(index, getStore());
    }

    @Override
    public IndexStatisticsGenerator<Key,byte[]> singleColumnVisitor(Session session, IndexColumn indexColumn) {
        return new FDBSingleColumnIndexStatisticsVisitor(getStore(), indexColumn);
    }


    //
    // Internal
    //

    protected IndexStatistics decodeHeader(KeyValue kv,
                                           RowDef indexStatisticsRowDef,
                                           Index index) {
        RowData rowData = new RowData();
        rowData.reset(kv.getValue());
        rowData.prepareRow(0);
        return decodeIndexStatisticsRow(rowData, indexStatisticsRowDef, index);
    }

    protected void decodeEntry(KeyValue kv,
                               RowDef indexStatisticsEntryRowDef,
                               IndexStatistics indexStatistics) {
        RowData rowData = new RowData();
        rowData.reset(kv.getValue());
        rowData.prepareRow(0);
        decodeIndexStatisticsEntryRow(rowData, indexStatisticsEntryRowDef, indexStatistics);
    }
}
