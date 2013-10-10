/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server.store;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.IndexRowComposition;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.persistitadapter.indexrow.PersistitIndexRowBuffer;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.geophile.Space;
import com.foundationdb.server.geophile.SpaceLatLon;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.PointTap;
import com.foundationdb.util.tap.Tap;

import java.math.BigDecimal;

class StoreGIHandler<SDType> {
    private static final PointTap UNNEEDED_DELETE_TAP = Tap.createCount("superfluous_delete");
    private static final TInstance NON_NULL_Z_TYPE = MNumeric.BIGINT.instance(false);

    public static enum GroupIndexPosition {
        ABOVE_SEGMENT,
        BELOW_SEGMENT,
        WITHIN_SEGMENT
    }

    public static enum Action {
        STORE,
        DELETE,
        CASCADE,
        CASCADE_STORE
    }

    private final AbstractStore<SDType> store;
    private final StoreAdapter adapter;
    private final UserTable sourceTable;
    private final PersistitIndexRowBuffer indexRow;
    private final Value zSource_t3 = new Value(MNumeric.BIGINT.instance(true));

    private StoreGIHandler(AbstractStore<SDType> store, StoreAdapter adapter, UserTable sourceTable) {
        this.store = store;
        this.adapter = adapter;
        this.indexRow = new PersistitIndexRowBuffer(store);
        this.sourceTable = sourceTable;
    }

    public static <SDType> StoreGIHandler forTable(AbstractStore<SDType> store, StoreAdapter adapter, UserTable userTable) {
        ArgumentValidation.notNull("userTable", userTable);
        return new StoreGIHandler<>(store, adapter, userTable);
    }

    public static <SDType> StoreGIHandler forBuilding(AbstractStore<SDType> store, StoreAdapter adapter) {
        return new StoreGIHandler<>(store, adapter, null);
    }

    public void handleRow(GroupIndex groupIndex, Row row, Action action) {
        GroupIndexPosition sourceRowPosition = positionWithinBranch(groupIndex, sourceTable);
        if (sourceRowPosition.equals(GroupIndexPosition.BELOW_SEGMENT)) {
            return; // nothing to do
        }

        int firstSpatialColumn = groupIndex.isSpatial() ? groupIndex.firstSpatialArgument() : -1;
        SDType storeData = store.createStoreData(adapter.getSession(), groupIndex.indexDef());
        try {
            store.resetForWrite(storeData, groupIndex, indexRow);
            IndexRowComposition irc = groupIndex.indexRowComposition();
            int nFields = irc.getLength();
            int f = 0;
            while(f < nFields) {
                assert irc.isInRowData(f);
                assert ! irc.isInHKey(f);
                if(f == firstSpatialColumn) {
                    copyZValueToIndexRow(groupIndex, row, irc);
                    f += groupIndex.dimensions();
                } else {
                    copyFieldToIndexRow(groupIndex, row, irc.getFieldPosition(f++));
                }
            }
            // Non-null values only required for UNIQUE indexes, which GIs cannot be
            assert !groupIndex.isUnique() : "Unexpected unique index: " + groupIndex;
            indexRow.close(null, null, false);
            indexRow.tableBitmap(tableBitmap(groupIndex, row));
            switch (action) {
                case CASCADE_STORE:
                case STORE:
                    store.store(adapter.getSession(), storeData);
                    store.sumAddGICount(adapter.getSession(), storeData, groupIndex, 1);
                break;
                case CASCADE:
                case DELETE:
                    boolean existed = store.clear(adapter.getSession(), storeData);
                    if(existed) {
                        store.sumAddGICount(adapter.getSession(), storeData, groupIndex, -1);
                    } else {
                        UNNEEDED_DELETE_TAP.hit();
                    }
                break;
                default:
                    throw new UnsupportedOperationException(action.name());
            }
        } finally {
            store.releaseStoreData(adapter.getSession(), storeData);
        }
    }

    public UserTable getSourceTable () {
        return sourceTable;
    }

    private void copyFieldToIndexRow(GroupIndex groupIndex, Row row, int flattenedIndex) {
        Column column = groupIndex.getColumnForFlattenedRow(flattenedIndex);
        indexRow.append(row.value(flattenedIndex), column.tInstance());
    }

    private void copyZValueToIndexRow(GroupIndex groupIndex, Row row, IndexRowComposition irc) {
        BigDecimal[] coords = new BigDecimal[Space.LAT_LON_DIMENSIONS];
        SpaceLatLon space = (SpaceLatLon) groupIndex.space();
        int firstSpatialColumn = groupIndex.firstSpatialArgument();
        boolean zNull = false;
        for(int d = 0; d < Space.LAT_LON_DIMENSIONS; d++) {
            if(!zNull) {
                ValueSource columnValue = row.value(irc.getFieldPosition(firstSpatialColumn + d));
                if (columnValue.isNull()) {
                    zNull = true;
                } else {
                    coords[d] = ((BigDecimalWrapper)columnValue.getObject()).asBigDecimal();
                }
            }
        }
        if (zNull) {
            zSource_t3.putNull();
            indexRow.append(zSource_t3, NON_NULL_Z_TYPE);
        } else {
            zSource_t3.putInt64(space.shuffle(coords));
            indexRow.append(zSource_t3, NON_NULL_Z_TYPE);
        }
    }

    // The group index row's value contains a bitmap indicating which of the tables covered by the index
    // have rows contributing to this index row. The leafmost table of the index is represented by bit
    // position 0.
    private long tableBitmap(GroupIndex groupIndex, Row row) {
        long result = 0;
        UserTable table = groupIndex.leafMostTable();
        UserTable end = groupIndex.rootMostTable().parentTable();
        while(table != null && !table.equals(end)) {
            if(row.containsRealRowOf(table)) {
                result |= (1 << table.getDepth());
            }
            table = table.parentTable();
        }
        return result;
    }

    private GroupIndexPosition positionWithinBranch(GroupIndex groupIndex, UserTable table) {
        final UserTable leafMost = groupIndex.leafMostTable();
        if(table == null) {
            return GroupIndexPosition.ABOVE_SEGMENT;
        }
        if(table.equals(leafMost)) {
            return GroupIndexPosition.WITHIN_SEGMENT;
        }
        if(table.isDescendantOf(leafMost)) {
            return GroupIndexPosition.BELOW_SEGMENT;
        }
        if(groupIndex.rootMostTable().equals(table)) {
            return GroupIndexPosition.WITHIN_SEGMENT;
        }
        return groupIndex.rootMostTable().isDescendantOf(table)
                ? GroupIndexPosition.ABOVE_SEGMENT
                : GroupIndexPosition.WITHIN_SEGMENT;
    }
}
