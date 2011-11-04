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

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.*;
import com.akiban.qp.persistitadapter.sort.Sorter;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.error.QueryCanceledException;
import com.akiban.server.error.QueryTimedOutException;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeLink;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;

import java.io.InterruptedIOException;

public class PersistitAdapter extends StoreAdapter
{
    // StoreAdapter interface

    @Override
    public GroupCursor newGroupCursor(GroupTable groupTable)
    {
        GroupCursor cursor;
        try {
            cursor = new PersistitGroupCursor(this, groupTable);
        } catch (PersistitException e) {
            handlePersistitException(e);
            throw new AssertionError();
        }
        return cursor;
    }

    @Override
    public Cursor newIndexCursor(Index index, IndexKeyRange keyRange, API.Ordering ordering, IndexScanSelector selector)
    {
        Cursor cursor;
        try {
            cursor = new PersistitIndexCursor(this, schema.indexRowType(index), keyRange, ordering, selector);
        } catch (PersistitException e) {
            handlePersistitException(e);
            throw new AssertionError();
        }
        return cursor;
    }

    @Override
    public Cursor sort(Cursor input, RowType rowType, API.Ordering ordering, Bindings bindings)
    {
        try {
            return new Sorter(this, input, rowType, ordering, bindings).sort();
        } catch (PersistitException e) {
            handlePersistitException(e);
            throw new AssertionError();
        }
    }

    @Override
    public void checkQueryCancelation(long queryStartMsec)
    {
        if (session.isCurrentQueryCanceled()) {
            throw new QueryCanceledException(session);
        }
        long queryTimeoutSec = config.queryTimeoutSec();
        if (queryTimeoutSec >= 0) {
            long runningTimeMsec = System.currentTimeMillis() - queryStartMsec;
            if (runningTimeMsec > queryTimeoutSec * 1000) {
                throw new QueryTimedOutException(runningTimeMsec);
            }
        }
    }

    @Override
    public HKey newHKey(RowType rowType)
    {
        return new PersistitHKey(this, rowType.hKey());
    }

    @Override
    public void updateRow(Row oldRow, Row newRow, Bindings bindings) {
        RowDef rowDef = (RowDef) oldRow.rowType().userTable().rowDef();
        Object rowDefNewRow = newRow.rowType().userTable().rowDef();
        if (rowDef != rowDefNewRow) {
            throw new IllegalArgumentException(String.format("%s != %s", rowDef, rowDefNewRow));
        }

        RowData oldRowData = rowData(rowDef, oldRow, bindings);
        RowData newRowData = rowData(rowDef, newRow, bindings);
        try {
            persistit.updateRow(session, oldRowData, newRowData, null);
        } catch (PersistitException e) {
            handlePersistitException(e);
            assert false;
        }
    }
    @Override
    public void writeRow (Row newRow, Bindings bindings) {
        RowDef rowDef = (RowDef)newRow.rowType().userTable().rowDef();
        RowData newRowData = rowData (rowDef, newRow, bindings);
        try {
            persistit.writeRow(session, newRowData);
        } catch (PersistitException e) {
            handlePersistitException(e);
            assert false;
        }
    }
    
    @Override
    public void deleteRow (Row oldRow, Bindings bindings) {
        RowDef rowDef = (RowDef)oldRow.rowType().userTable().rowDef();
        RowData oldRowData = rowData(rowDef, oldRow, bindings);
        try {
            persistit.deleteRow(session, oldRowData);
        } catch (PersistitException e) {
            handlePersistitException(e);
            assert false;
        }
    }

    // PersistitAdapter interface

    public RowDef rowDef(int tableId)
    {
        return persistit.getRowDefCache().getRowDef(tableId);
    }

    public NewRow newRow(RowDef rowDef)
    {
        NiceRow row = new NiceRow(rowDef.getRowDefId(), rowDef);
        UserTable table = rowDef.userTable();
        PrimaryKey primaryKey = table.getPrimaryKeyIncludingInternal();
        if (primaryKey != null && table.getPrimaryKey() == null) {
            // Akiban-generated PK. Initialize its value to a dummy value, which will be replaced later. The
            // important thing is that the value be non-null.
            row.put(table.getColumnsIncludingInternal().size() - 1, -1L);
        }
        return row;
    }

    public RowData rowData(RowDef rowDef, RowBase row, Bindings bindings)
    {
        if (row instanceof PersistitGroupRow) {
            return ((PersistitGroupRow) row).rowData();
        }
        ToObjectValueTarget target = new ToObjectValueTarget();
        NewRow niceRow = newRow(rowDef);
        for(int i = 0; i < row.rowType().nFields(); ++i) {
            ValueSource source = row.eval(i);
            niceRow.put(i, target.convertFromSource(source));
        }
        return niceRow.toRowData();
    }

    public PersistitGroupRow newGroupRow()
    {
        return PersistitGroupRow.newPersistitGroupRow(this);
    }

    public PersistitIndexRow newIndexRow(IndexRowType indexRowType) throws PersistitException
    {
        return new PersistitIndexRow(this, indexRowType);
    }


    public Exchange takeExchange(GroupTable table) throws PersistitException
    {
        return persistit.getExchange(session, (RowDef) table.rowDef());
    }

    public Exchange takeExchange(Index index)
    {
        return persistit.getExchange(session, index);
    }

    public Exchange takeExchangeForSorting(TreeLink treeLink)
    {
        return treeService.getExchange(session, treeLink);
    }

    public Key newKey()
    {
        return new Key(persistit.getDb());
    }

    public void handlePersistitException(PersistitException e)
    {
        if (e instanceof PersistitInterruptedException ||
            e.getCause() != null && e.getCause() instanceof InterruptedIOException) {
            throw new QueryCanceledException(session);
        } else {
            throw new PersistitAdapterException(e);
        }
    }

    public void returnExchange(Exchange exchange)
    {
        persistit.releaseExchange(session, exchange);
    }

    public PersistitAdapter(Schema schema,
                            PersistitStore persistit,
                            TreeService treeService,
                            Session session,
                            ConfigurationService config)
    {
        this(schema, persistit, session, treeService, config, null);
    }

    PersistitAdapter(Schema schema,
                     PersistitStore persistit,
                     Session session,
                     TreeService treeService,
                     ConfigurationService config,
                     PersistitFilterFactory.InternalHook hook)
    {
        super(schema);
        this.config = config;
        this.persistit = persistit;
        this.session = session;
        this.treeService = treeService;
        this.filterFactory = new PersistitFilterFactory(this, hook);
    }

    // Object state

    private final TreeService treeService;
    private final ConfigurationService config;
    final PersistitStore persistit;
    final Session session;
    final PersistitFilterFactory filterFactory;
}
