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

package com.foundationdb.qp.storeadapter.indexcursor;

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.BindingsAwareCursor;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.InternalIndexTypes;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.api.dml.IndexRowPrefixSelector;
import com.foundationdb.server.spatial.GeophileCursor;
import com.foundationdb.server.spatial.GeophileIndex;
import com.foundationdb.server.spatial.TransformingIterator;
import com.foundationdb.server.types.texpressions.TPreparedField;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueRecord;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.util.IteratorToCursorAdapter;
import com.geophile.z.Cursor;
import com.geophile.z.Pair;
import com.geophile.z.Record;
import com.geophile.z.Space;
import com.geophile.z.SpatialIndex;
import com.geophile.z.SpatialJoin;
import com.geophile.z.SpatialObject;
import com.geophile.z.index.RecordWithSpatialObject;
import com.geophile.z.index.sortedarray.SortedArray;
import com.geophile.z.space.SpaceImpl;
import com.geophile.z.spatialobject.jts.JTS;
import com.vividsolutions.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

// A scan of an IndexCursorSpatial_InBox will be implemented as one or more IndexCursorUnidirectional scans.

public class IndexCursorSpatial_InBox extends IndexCursor
{
    @Override
    public void open()
    {
        super.open();
        // Close any underlying store data still open
        iterationHelper.closeIteration();
        createSpatialJoinCursor();
        geophileCursor.rebind(bindings);
        spatialJoinCursor.open();
    }

    @Override
    public Row next()
    {
        super.next();
        return spatialJoinCursor.next();
    }

    @Override
    public void close()
    {
        super.close();
        spatialJoinCursor.close();
    }

    // IndexCursorSpatial_InBox interface

    public static IndexCursorSpatial_InBox create(QueryContext context,
                                                  IterationHelper iterationHelper,
                                                  IndexKeyRange keyRange,
                                                  boolean openAll)
    {
        return new IndexCursorSpatial_InBox(context, iterationHelper, keyRange, openAll);
    }

    // For use by this class

    private IndexCursorSpatial_InBox(final QueryContext context,
                                     IterationHelper iterationHelper,
                                     final IndexKeyRange keyRange,
                                     final boolean openEarly)
    {
        super(context, iterationHelper);
        this.keyRange = keyRange;
        this.index = keyRange.indexRowType().index();
        assert index.isSpatial() : index;
        this.space = this.index.space();
        this.openEarly = openEarly;
    }

    private void createSpatialJoinCursor()
    {
        this.loExpressions = keyRange.lo().boundExpressions(context, bindings);
        final SpatialObject spatialObject = spatialObject();
        // Set up spatial join and iterator over spatial join output
        SpatialJoin spatialJoin =
            SpatialJoin.newSpatialJoin(SPATIAL_JOIN_DUPLICATION,
                                       null,
                                       SPATIAL_JOIN_LEFT_OBSERVER,
                                       LOG.isDebugEnabled()
                                       ? SPATIAL_JOIN_RIGHT_DEBUG_OBSERVER
                                       : SPATIAL_JOIN_RIGHT_OBSERVER);
        Iterator<Pair<RecordWithSpatialObject, IndexRow>> spatialJoinIterator;
        try {
            // Set up spatial index over the index
            SpatialIndex<IndexRow> dataSpatialIndex =
                SpatialIndex.newSpatialIndex(space, dataIndex(context, openEarly, spatialObject));
            // Set up spatial index over query object
            SortedArray<RecordWithSpatialObject> queryIndex = new SortedArray.OfBaseRecord();
            SpatialIndex<RecordWithSpatialObject> querySpatialIndex = SpatialIndex.newSpatialIndex(space, queryIndex);
            Record.Factory<RecordWithSpatialObject> recordFactory =
                new Record.Factory<RecordWithSpatialObject>()
                {
                    @Override
                    public RecordWithSpatialObject newRecord()
                    {
                        RecordWithSpatialObject record = new RecordWithSpatialObject();
                        record.spatialObject(spatialObject);
                        return record;
                    }
                };
            querySpatialIndex.add(spatialObject, recordFactory, MAX_Z);
            spatialJoinIterator = spatialJoin.iterator(querySpatialIndex, dataSpatialIndex);
            if (LOG.isDebugEnabled()) {
                logQueryIndex(queryIndex);
            }
        } catch (IOException | InterruptedException e) {
            // These exceptions are declared by Geophile, but Geophile sits on top of FDB which should be
            // doing the right thing.
            throw new IllegalStateException(e);
        }
        spatialJoinCursor =
            new IteratorToCursorAdapter(
                new TransformingIterator<Pair<RecordWithSpatialObject, IndexRow>, IndexRow>(spatialJoinIterator)
                {
                    @Override
                    public IndexRow transform(Pair<RecordWithSpatialObject, IndexRow> pair)
                    {
                        return pair.right();
                    }
                });
    }

    private GeophileIndex dataIndex(final QueryContext context,
                                    final boolean openEarly,
                                    final SpatialObject spatialObject)
    {
        final API.Ordering zOrdering = new API.Ordering();
        IndexRowType rowType = keyRange.indexRowType().physicalRowType();
        for (int f = 0; f < rowType.nFields(); f++) {
            zOrdering.append(new TPreparedField(rowType.typeAt(f), f), true);
        }
        GeophileIndex.CursorFactory cursorFactory = new GeophileIndex.CursorFactory()
        {
            @Override
            public GeophileCursor newCursor(GeophileIndex geophileIndex)
            {
                // About the assignment of a GeophileCursor to IndexCursorSpatial_InBox.geophileCursor:
                // The GeophileCursor has to be returned by GeophileIndex.CursorFactory.newCursor.
                // It is also needed by this class to support rebind.
                geophileCursor = new GeophileCursor(geophileIndex, openEarly);
                for (Map.Entry<Long, IndexKeyRange> entry : zKeyRanges(keyRange, spatialObject).entrySet()) {
                    long z = entry.getKey();
                    IndexKeyRange zKeyRange = entry.getValue();
                    IterationHelper rowState = adapter.createIterationHelper(keyRange.indexRowType());
                    IndexCursorUnidirectional<ValueSource> zIntervalCursor =
                        new IndexCursorUnidirectional<>(context,
                                                        rowState,
                                                        zKeyRange,
                                                        zOrdering,
                                                        ValueSortKeyAdapter.INSTANCE);
                    geophileCursor.addCursor(z, zIntervalCursor);
                }
                return geophileCursor;
            }
        };
        return new GeophileIndex(adapter, keyRange.indexRowType(), cursorFactory);
    }

    private Map<Long, IndexKeyRange> zKeyRanges(IndexKeyRange keyRange, SpatialObject spatialObject)
    {
        Map<Long, IndexKeyRange> zKeyRanges = new HashMap<>();
        // The index column selector needs to select all the columns before the z column, and the z column itself.
        ColumnSelector indexColumnSelector = new IndexRowPrefixSelector(this.index.firstSpatialArgument() + 1);
        long[] zValues = new long[MAX_Z];
        space.decompose(spatialObject, zValues);
        int zColumn = index.firstSpatialArgument();
        Value toEnd = new Value(InternalIndexTypes.LONG.instance(false));
        toEnd.putInt64(Long.MAX_VALUE);
        for (int i = 0; i < zValues.length && zValues[i] != SpaceImpl.Z_NULL; i++) {
            long z = zValues[i];
            // Need to do an index lookup for z and each ancestor
            while (z != SpaceImpl.Z_NULL) {
                IndexKeyRange zKeyRange = zKeyRanges.get(z);
                if (zKeyRange == null) {
                    IndexRowType physicalRowType = keyRange.indexRowType().physicalRowType();
                    int indexRowFields = physicalRowType.nFields();
                    SpatialIndexValueRecord zLoRow = new SpatialIndexValueRecord(indexRowFields);
                    SpatialIndexValueRecord zHiRow = new SpatialIndexValueRecord(indexRowFields);
                    IndexBound zLo = new IndexBound(zLoRow, indexColumnSelector);
                    IndexBound zHi = new IndexBound(zHiRow, indexColumnSelector);
                    // Take care of any equality restrictions before the spatial fields
                    for (int f = 0; f < zColumn; f++) {
                        ValueSource eqValueSource = loExpressions.value(f);
                        zLoRow.value(f, eqValueSource);
                        zHiRow.value(f, eqValueSource);
                    }
                    // lo and hi bounds
                    Value loValue = new Value(InternalIndexTypes.LONG.instance(false));
                    loValue.putInt64(Space.zLo(z));
                    zLoRow.value(zColumn, loValue);
                    zHiRow.value(zColumn, toEnd);
                    zKeyRange = IndexKeyRange.bounded(physicalRowType, zLo, true, zHi, true);
                    zKeyRanges.put(z, zKeyRange);
                    z = z == SpaceImpl.Z_MIN ? SpaceImpl.Z_NULL : SpaceImpl.parent(z);
                } else {
                    // If z is present, then so are all of its ancestors
                    z = SpaceImpl.Z_NULL;
                }
            }
        }
        return zKeyRanges;
    }

    private SpatialObject spatialObject()
    {
        ValueRecord expressions = keyRange.lo().boundExpressions(context, bindings);
        Object object = expressions.value(index.firstSpatialArgument()).getObject();
        if (object instanceof SpatialObject) {
            return (SpatialObject) object;
        } else if (object instanceof Geometry) {
            return JTS.spatialObject(space, (Geometry) object);
        } else {
            throw new IllegalStateException(String.format("Expected SpatialObject, found: %s",
                                                          object.getClass()));
        }
    }

    private void logQueryIndex(SortedArray<RecordWithSpatialObject> queryIndex)
        throws IOException, InterruptedException
    {
        LOG.debug("Query index:");
        Cursor<RecordWithSpatialObject> queryCursor = queryIndex.cursor();
        RecordWithSpatialObject zMinRecord = queryIndex.newRecord();
        zMinRecord.z(SpaceImpl.Z_MIN);
        queryCursor.goTo(zMinRecord);
        RecordWithSpatialObject queryRecord;
        while ((queryRecord = queryCursor.next()) != null) {
            LOG.debug("    {}", SpaceImpl.formatZ(queryRecord.z()));
        }
    }

    // Class state

    private static final SpatialJoin.Duplicates SPATIAL_JOIN_DUPLICATION = SpatialJoin.Duplicates.EXCLUDE;
    public static final int MAX_Z = 4;
    // SPATIAL_JOIN_LEFT/RIGHT_OBSERVERs are public so that they can be set by ITs
    public static SpatialJoin.InputObserver SPATIAL_JOIN_LEFT_OBSERVER = null;
    public static SpatialJoin.InputObserver SPATIAL_JOIN_RIGHT_OBSERVER = null;
    private static SpatialJoin.InputObserver SPATIAL_JOIN_RIGHT_DEBUG_OBSERVER =
        new SpatialJoin.InputObserver()
        {
            @Override public void randomAccess(Cursor cursor, long z)
            {
                LOG.debug("Random access using {}: {}", cursor, SpaceImpl.formatZ(z));
            }

            @Override
            public void sequentialAccess(Cursor cursor, long zRandomAccess, Record record)
            {
                LOG.debug("    Sequential access using {} {} -> {}: {}",
                          cursor,
                          SpaceImpl.formatZ(zRandomAccess),
                          record == null ? SpaceImpl.Z_NULL : SpaceImpl.formatZ(record.z()),
                          record);
            }
        };

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(IndexCursorSpatial_InBox.class);

    // Object state

    private final Space space;
    private final Index index;
    private final IndexKeyRange keyRange;
    private final boolean openEarly;
    private ValueRecord loExpressions;
    private GeophileCursor geophileCursor;
    private BindingsAwareCursor spatialJoinCursor;
}
