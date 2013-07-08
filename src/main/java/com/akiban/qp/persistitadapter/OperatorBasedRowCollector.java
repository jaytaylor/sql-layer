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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.*;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Limit;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryBindings;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.*;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.api.FixedCountLimit;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.PredicateLimit;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.rowdata.IndexDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.RowCollector;
import com.akiban.server.store.Store;
import com.akiban.server.types3.Types3Switch;
import com.akiban.util.GrowableByteBuffer;
import com.akiban.util.ShareHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.akiban.qp.operator.API.*;

public abstract class OperatorBasedRowCollector implements RowCollector
{
    // RowCollector interface

    @Override
    public void open() {
        if (cursor != null) {
            throw new IllegalStateException("cursor is already open");
        }
        QueryContext context = new SimpleQueryContext(adapter);
        QueryBindings bindings = context.createBindings();
        cursor = cursor(operator, context, bindings);
        cursor.open();
        // closed was initialized to true, because hasMore is checked before open. (This is due to scan being
        // spread across possibly multiple requests.) Now set closed to false for the actual scanning of rows.
        closed = false;
    }

    @Override
    public boolean collectNextRow(GrowableByteBuffer payload)
    {
         // The handling of currentRow is slightly tricky: If writing to the payload results in BufferOverflowException,
         // then there is likely to be another call of this method, expecting to get the same row and write it into
         // another payload with room, (resulting from a ScanRowsMoreRequest). currentRow is used only to hold onto
         // the current row across these two invocations.
        boolean wasHeld = false;
        boolean wroteToPayload = false;
        AbstractRow row;
        if (currentRow.isEmpty()) {
            row = (AbstractRow) cursor.next();
        } else {
            wasHeld = true;
            row = (AbstractRow) currentRow.get();
            currentRow.release();
        }
        if (row == null) {
            close();
        } else {
            boolean doHold = false;
            RowData rowData = row.rowData();

            // Only grow past cache size if we haven't written a single row
            if (rowCount == 0 || wasHeld || (payload.position() + rowData.getRowSize() < payload.getMaxCacheSize())) {
                try {
                    payload.put(rowData.getBytes(), rowData.getRowStart(), rowData.getRowSize());
                    wroteToPayload = true;
                    rowCount++;
                } catch (BufferOverflowException e) {
                    doHold = true;
                }
            } else {
                doHold = true;
            }

            if (doHold) {
                assert !wroteToPayload;
                currentRow.hold(row);
            }
        }
        return wroteToPayload;
    }

    @Override
    public RowData collectNextRow()
    {
        RowData rowData = null;
        AbstractRow row = (AbstractRow) cursor.next();
        if (row == null) {
            close();
        } else {
            currentRow.hold(row);
            rowData = row.rowData();
            rowCount++;
        }
        return rowData;
    }

    @Override
    public boolean hasMore()
    {
        return !closed;
    }

    @Override
    public void close()
    {
        if (!closed) {
            currentRow.release();
            if (cursor != null) {
                cursor.destroy();
                cursor = null;
            }
            closed = true;
        }
    }

    @Override
    public int getDeliveredRows()
    {
        return rowCount;
    }

    @Override
    public int getDeliveredBuffers()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDeliveredBytes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTableId()
    {
        return predicateType.userTable().getTableId();
    }

    @Override
    public IndexDef getIndexDef()
    {
        return predicateIndex == null ? null : predicateIndex.indexDef();
    }

    @Override
    public long getId()
    {
        return rowCollectorId;
    }

    @Override
    public void outputToMessage(boolean outputToMessage)
    {
    }

    @Override
    public boolean checksLimit()
    {
        return true;
    }

    // OperatorBasedRowCollector interface

    public static OperatorBasedRowCollector newCollector(Session session,
                                                         Store store,
                                                         int scanFlags,
                                                         RowDef rowDef,
                                                         int indexId,
                                                         byte[] columnBitMap,
                                                         RowData start,
                                                         ColumnSelector startColumns,
                                                         RowData end,
                                                         ColumnSelector endColumns,
                                                         ScanLimit scanLimit)
    {
        if (start != null && end != null && start.getRowDefId() != end.getRowDefId()) {
            throw new IllegalArgumentException(String.format("start row def id: %s, end row def id: %s",
                                                             start.getRowDefId(), end.getRowDefId()));
        }
        if(!rowDef.isUserTable()) {
            throw new IllegalArgumentException("Must scan a UserTable: " + rowDef);
        }
        OperatorBasedRowCollector rowCollector =
              new OneTableRowCollector(session,
                                       store,
                                       rowDef,
                                       indexId,
                                       scanFlags,
                                       start,
                                       startColumns,
                                       end,
                                       endColumns);
        boolean singleRow = (scanFlags & SCAN_FLAGS_SINGLE_ROW) != 0;
        boolean descending = (scanFlags & SCAN_FLAGS_DESCENDING) != 0;
        boolean deep = (scanFlags & SCAN_FLAGS_DEEP) != 0;
        rowCollector.createPlan(scanLimit, singleRow, descending, deep);
        return rowCollector;
    }
    
    protected OperatorBasedRowCollector(Store store, Session session)
    {
        this.schema = SchemaCache.globalSchema(store.getAIS(session));
        this.adapter = store.createAdapter(session, schema);
        this.rowCollectorId = idCounter.getAndIncrement();
    }

    protected static ColumnSelector indexSelectorFromTableSelector(Index index, final ColumnSelector tableSelector)
    {
        assert index.isTableIndex() : index;
        final IndexRowComposition rowComp = index.indexRowComposition();
        return
            new ColumnSelector()
            {
                @Override
                public boolean includesColumn(int columnPosition)
                {
                    int tablePos = rowComp.getFieldPosition(columnPosition);
                    return tableSelector.includesColumn(tablePos);
                }
            };
    }

    private void createPlan(ScanLimit scanLimit, boolean singleRow, boolean descending, boolean deep)
    {
        // Plan and query
        Limit limit = new PersistitRowLimit(scanLimit(scanLimit, singleRow));
        boolean useIndex = predicateIndex != null;
        Group group = queryRootTable.getGroup();
        Operator plan;
        if (useIndex) {
            IndexRowType indexRowType = predicateType.indexRowType(predicateIndex).physicalRowType();
            Operator indexScan = indexScan_Default(indexRowType,
                                                   descending,
                                                   indexKeyRange);
            plan = branchLookup_Default(indexScan,
                    group,
                    indexRowType,
                    predicateType,
                    InputPreservationOption.DISCARD_INPUT,
                    limit);
        } else {
            assert !descending;
            plan = groupScan_Default(group);
            if (scanLimit != ScanLimit.NONE) {
                if (scanLimit instanceof FixedCountLimit) {
                    plan = limit_Default(plan, ((FixedCountLimit) scanLimit).getLimit());
                } else if (scanLimit instanceof PredicateLimit) {
                    plan = limit_Default(plan, ((PredicateLimit) scanLimit).getLimit());
                }
            }
        }
        // Fill in ancestors above predicate
        if (queryRootType != predicateType) {
            List<UserTableRowType> ancestorTypes = ancestorTypes();
            if (!ancestorTypes.isEmpty()) {
                plan = ancestorLookup_Default(plan, group, predicateType, ancestorTypes, InputPreservationOption.KEEP_INPUT);
            }
        }
        // Get rid of everything above query root table.
        if (queryRootTable.parentTable() != null) {
            Set<RowType> queryRootAndDescendents = Schema.descendentTypes(queryRootType, schema.userTableTypes());
            queryRootAndDescendents.add(queryRootType);
            plan = filter_Default(plan, queryRootAndDescendents);
        }
        // Get rid of selected types below query root table.
        Set<AisRowType> cutTypes = cutTypes(deep);
        for (AisRowType cutType : cutTypes) {
            plan = filter_Default(plan, removeDescendentTypes(cutType, plan));
        }
        if (LOG.isInfoEnabled()) {
            LOG.debug("Execution plan:\n{}", plan.describePlan());
        }
        this.operator = plan;
    }

    private Set<RowType> removeDescendentTypes(AisRowType type, Operator plan)
    {
        Set<RowType> keepTypes = type.schema().allTableTypes();
        plan.findDerivedTypes(keepTypes);
        keepTypes.removeAll(Schema.descendentTypes(type, keepTypes));
        return keepTypes;
    }

    private List<UserTableRowType> ancestorTypes()
    {
        UserTableRowType queryRootType = schema.userTableRowType(queryRootTable);
        List<UserTableRowType> ancestorTypes = new ArrayList<>();
        if (predicateType != null && queryRootType != predicateType) {
            UserTable ancestor = predicateType.userTable();
            do {
                ancestor = ancestor.parentTable();
                ancestorTypes.add(schema.userTableRowType(ancestor));
            } while (ancestor != queryRootType.userTable());
        }
        return ancestorTypes;
    }

    private Set<AisRowType> cutTypes(boolean deep)
    {
        Set<AisRowType> cutTypes = new HashSet<>();
        if (!deep) {
            // Find the leafmost tables in requiredUserTables and cut everything below those. It is possible
            // that a column bit map includes, for example, customer and item but not order. This case is NOT
            // handled -- we'll just include (i.e. not cut) customer, order and item.
            Set<UserTable> leafmostRequiredUserTables = new HashSet<>(requiredUserTables);
            for (UserTable requiredUserTable : requiredUserTables) {
                UserTable ancestor = requiredUserTable.parentTable();
                while (ancestor != null) {
                    leafmostRequiredUserTables.remove(ancestor);
                    ancestor = ancestor.parentTable();
                }
            }
            // Cut below each leafmost required table
            for (UserTable leafmostRequiredUserTable : leafmostRequiredUserTables) {
                cutTypes.add(schema.userTableRowType(leafmostRequiredUserTable));
            }
        }
        if (predicateType != null) {
            UserTable predicateTable = predicateType.userTable();
            if (predicateTable != queryRootTable) {
                // Cut tables not on the path from the predicate table up to query table
                UserTable table = predicateTable;
                UserTable childOnPath;
                while (table != queryRootTable) {
                    childOnPath = table;
                    table = table.parentTable();
                    for (Join join : table.getChildJoins()) {
                        UserTable child = join.getChild();
                        if (child != childOnPath) {
                            cutTypes.add(schema.userTableRowType(child));
                        }
                    }
                }
            }
        }
        return cutTypes;
    }

    private ScanLimit scanLimit(ScanLimit requestLimit, boolean singleRow)
    {
        ScanLimit limit = requestLimit == null ? ScanLimit.NONE : requestLimit;
        if (limit != ScanLimit.NONE && singleRow) {
            throw new IllegalArgumentException
                ("Cannot specify limit along with SCAN_FLAGS_SINGLE_ROW");
        }
        if (singleRow) {
            limit = new PredicateLimit(predicateType.userTable().getTableId(), 1);
        }
        return limit;
    }

    // Class state

    private static final AtomicLong idCounter = new AtomicLong(0);
    private static final Logger LOG = LoggerFactory.getLogger(OperatorBasedRowCollector.class);

    // Object state

    private long rowCollectorId;
    protected final Schema schema;
    protected StoreAdapter adapter;
    protected UserTable queryRootTable;
    protected UserTableRowType queryRootType;
    protected TableIndex predicateIndex;
    protected UserTableRowType predicateType;
    // If we're querying a user table, then requiredUse
    // rTables contains just queryRootTable
    // If we're querying a group table, it contains those user tables containing columns in the
    // columnBitMap.
    private Operator operator;
    protected final Set<UserTable> requiredUserTables = new HashSet<>();
    protected IndexKeyRange indexKeyRange;
    private Cursor cursor;
    private int rowCount = 0;
    private ShareHolder<Row> currentRow = new ShareHolder<>();
    private boolean closed = true; // Not false, so that initial call to hasMore, prior to open, will proceed to call open.
    private boolean usePVals = Types3Switch.ON;

//    // inner class
//    static class OpenInfoStruct {
//        final ScanLimit scanLimit;
//        final boolean singleRow;
//        final boolean descending;
//        final boolean deep;
//
//        private OpenInfoStruct(ScanLimit scanLimit, boolean singleRow, boolean descending, boolean deep) {
//            this.scanLimit = scanLimit;
//            this.singleRow = singleRow;
//            this.descending = descending;
//            this.deep = deep;
//        }
//    }
}
