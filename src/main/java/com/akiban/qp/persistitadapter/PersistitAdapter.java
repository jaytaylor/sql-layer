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
import com.akiban.qp.persistitadapter.sort.Sorter;
import com.akiban.qp.operator.*;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.error.PersistItErrorException;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeLink;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;
import com.persistit.Exchange;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

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
            throw new StoreAdapterRuntimeException(e);
        }
        return cursor;
    }

    @Override
    public Cursor newIndexCursor(Index index, boolean reverse, IndexKeyRange keyRange, UserTable innerJoinUntil)
    {
        Cursor cursor;
        try {
            cursor = new PersistitIndexCursor(this, schema.indexRowType(index), reverse, keyRange, innerJoinUntil);
        } catch (PersistitException e) {
            throw new StoreAdapterRuntimeException(e);
        }
        return cursor;
    }

    @Override
    public Cursor sort(Cursor input, RowType rowType, API.Ordering ordering, Bindings bindings)
    {
        try {
            return new Sorter(this, input, rowType, ordering, bindings).sort();
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    @Override
    public void checkQueryCancelation()
    {
        if (session.isCurrentQueryCanceled()) {
            throw new QueryCanceledException();
        }
    }

    @Override
    public HKey newHKey(RowType rowType)
    {
        return new PersistitHKey(this, rowType.hKey());
    }

    public void setTransactional(boolean transactional)
    {
        this.transactional.set(transactional);
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
            throw new PersistItErrorException(e);
        }
    }
    @Override
    public void writeRow (Row newRow, Bindings bindings) {
        RowDef rowDef = (RowDef)newRow.rowType().userTable().rowDef();
        RowData newRowData = rowData (rowDef, newRow, bindings);
        try {
            persistit.writeRow(session, newRowData);
        } catch (PersistitException e) {
            throw new PersistItErrorException (e);
        }
    }
    
    @Override
    public void deleteRow (Row oldRow, Bindings bindings) {
        RowDef rowDef = (RowDef)oldRow.rowType().userTable().rowDef();
        RowData oldRowData = rowData(rowDef, oldRow, bindings);
        try {
            persistit.deleteRow(session, oldRowData);
        } catch (PersistitException e) {
            throw new PersistItErrorException (e);
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
        return transact(persistit.getExchange(session, (RowDef) table.rowDef()));
    }

    public Exchange takeExchange(Index index)
    {
        return transact(persistit.getExchange(session, index));
    }

    public Exchange takeExchangeForSorting()
    {
        return treeService.getExchange(session, sortTreeLink);
    }

    private Exchange transact(Exchange exchange)
    {
        if (transactional.get()) {
            synchronized (transactionsMap) {
                if (!transactionsMap.containsKey(exchange)) {
                    Transaction transaction = exchange.getTransaction();
                    try {
                        transaction.begin();
                    } catch (PersistitException e) {
                        throw new RuntimeException(e);
                    }
                    transactionsMap.put(exchange, transaction);
                }
            }
        }
        return exchange;
    }

    public void commitAllTransactions() throws PersistitException
    {
        Collection<Transaction> transactions = new ArrayList<Transaction>();
        synchronized (transactionsMap) {
            Iterator<Transaction> transactionsIter = transactionsMap.values().iterator();
            while (transactionsIter.hasNext()) {
                transactions.add(transactionsIter.next());
                transactionsIter.remove();
            }
        }
        for (Transaction transaction : transactions) {
            transaction.commit();
            transaction.end();
        }
    }

    public void returnExchange(Exchange exchange)
    {
        persistit.releaseExchange(session, exchange);
    }

    public PersistitAdapter(Schema schema,
                            PersistitStore persistit,
                            TreeService treeService,
                            Session session)
    {
        this(schema, persistit, session, treeService, null);
    }

    PersistitAdapter(Schema schema,
                     PersistitStore persistit,
                     Session session,
                     TreeService treeService,
                     PersistitFilterFactory.InternalHook hook)
    {
        super(schema);
        this.persistit = persistit;
        this.session = session;
        this.treeService = treeService;
        this.filterFactory = new PersistitFilterFactory(this, hook);
        this.sortTreeLink = new TemporaryTableTreeLink(SORT_TREE_NAME_PREFIX + session.sessionId());
    }

    // Class state

    private static final String SORT_TREE_NAME_PREFIX = "sort.";

    // Object state

    private final AtomicBoolean transactional = new AtomicBoolean(false);
    private final Map<Exchange, Transaction> transactionsMap = new HashMap<Exchange, Transaction>();
    private final TreeService treeService;
    private final TreeLink sortTreeLink;
    final PersistitStore persistit;
    final Session session;
    final PersistitFilterFactory filterFactory;
}
