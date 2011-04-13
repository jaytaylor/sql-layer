/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.*;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.physicaloperator.Cursor;
import com.akiban.qp.physicaloperator.Executable;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.row.ManagedRow;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.IndexDef;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.RowCollector;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.akiban.qp.physicaloperator.API.*;

public abstract class OperatorBasedRowCollector implements RowCollector
{
    // RowCollector interface

    @Override
    public boolean collectNextRow(ByteBuffer payload) throws Exception
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowData collectNextRow() throws Exception
    {
        RowData rowData = null;
        if (!closed) {
            currentRow.set(cursor.currentRow());
            PersistitGroupRow row = (PersistitGroupRow) currentRow.managedRow();
            assert row != null;
            rowData = row.rowData();
            rowCount++;
            if (!cursor.next()) {
                currentRow.set(null);
                close();
            }
        }
        return rowData;
    }

    @Override
    public boolean hasMore() throws Exception
    {
        return !closed;
    }

    @Override
    public void close()
    {
        if (!closed) {
            cursor.close();
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
    public int getRepeatedRows()
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
        return (IndexDef) predicateIndex.indexDef();
    }

    @Override
    public long getId()
    {
        return rowCollectorId;
    }

    @Override
    public void outputToMessage(boolean outputToMessage)
    {
        assert !outputToMessage;
    }

    // OperatorBasedRowCollector interface

    public static OperatorBasedRowCollector newCollector(Session session,
                                                         PersistitStore store,
                                                         RowDef rowDef,
                                                         int indexId,
                                                         int scanFlags,
                                                         RowData start,
                                                         RowData end,
                                                         byte[] columnBitMap)
    {
        if ((scanFlags & (SCAN_FLAGS_PREFIX | SCAN_FLAGS_SINGLE_ROW | SCAN_FLAGS_DESCENDING)) != 0) {
            throw new IllegalArgumentException
                ("SCAN_FLAGS_PREFIX, SCAN_FLAGS_SINGLE_ROW and SCAN_FLAGS_DESCENDING are unsupported");
        }
        if ((scanFlags & SCAN_FLAGS_DEEP) == 0) {
            throw new IllegalArgumentException("SCAN_FLAGS_DEEP is required");
        }
        if (start != null && end != null && start.getRowDefId() != end.getRowDefId()) {
            throw new IllegalArgumentException(String.format("start row def id: %s, end row def id: %s",
                                                             start.getRowDefId(), end.getRowDefId()));
        }
        OperatorBasedRowCollector rowCollector =
            rowDef.isUserTable()
            // HAPI query root table = predicate table
            ? new OneTableRowCollector(session, store, rowDef, indexId, scanFlags, start, end)
            // HAPI query root table != predicate table
            : new TwoTableRowCollector(session, store, rowDef, indexId, scanFlags, start, end, columnBitMap);
        rowCollector.createPlan();
        return rowCollector;
    }
    
    protected OperatorBasedRowCollector(PersistitStore store, Session session)
    {
        AkibanInformationSchema ais = store.getRowDefCache().ais();
        this.schema = new Schema(ais);
        this.adapter = new PersistitAdapter(this.schema, store, session);
        this.rowCollectorId = idCounter.getAndIncrement();
    }

    private void createPlan()
    {
        // Plan and query
        Executable query;
        boolean hKeyEquivalentIndex = ((IndexDef) predicateIndex.indexDef()).isHKeyEquivalent();
        if (hKeyEquivalentIndex) {
            PhysicalOperator groupScan = groupScan_Default(adapter, queryRootTable.getGroup().getGroupTable());
            query = new Executable(adapter, groupScan).bind(groupScan, indexKeyRange);
        } else {
            PhysicalOperator indexScan = indexScan_Default(predicateIndex);
            PhysicalOperator indexLookup = indexLookup_Default(indexScan,
                                                               predicateIndex.getTable().getGroup().getGroupTable(),
                                                               ancestorTypes());
            query = new Executable(adapter, indexLookup).bind(indexScan, indexKeyRange);
        }
        // Executable stuff
        cursor = query.cursor();
        cursor.open();
        closed = !cursor.next();
    }

    private List<RowType> ancestorTypes()
    {
        UserTableRowType queryRootType = schema.userTableRowType(queryRootTable);
        List<RowType> ancestorTypes = new ArrayList<RowType>();
        if (queryRootType != predicateType) {
            UserTable ancestor = predicateType.userTable();
            do {
                ancestor = ancestor.parentTable();
                ancestorTypes.add(schema.userTableRowType(ancestor));
            } while (ancestor != queryRootType.userTable());
        }
        return ancestorTypes;
    }

    // Class state

    private static final AtomicLong idCounter = new AtomicLong(0);

    // Object state

    private long rowCollectorId;
    protected PersistitAdapter adapter;
    protected Schema schema;
    protected UserTable queryRootTable;
    protected Index predicateIndex;
    protected UserTableRowType predicateType;
    protected IndexKeyRange indexKeyRange;
    private Cursor cursor;
    private boolean closed;
    private int rowCount = 0;
    private RowHolder<ManagedRow> currentRow = new RowHolder<ManagedRow>();
}
