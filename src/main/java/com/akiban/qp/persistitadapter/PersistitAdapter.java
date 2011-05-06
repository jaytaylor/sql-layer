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
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.physicaloperator.Cursor;
import com.akiban.qp.physicaloperator.GroupCursor;
import com.akiban.qp.physicaloperator.StoreAdapter;
import com.akiban.qp.physicaloperator.StoreAdapterRuntimeException;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.IndexDef;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.api.dml.DuplicateKeyException;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.PersistitStore;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class PersistitAdapter extends StoreAdapter
{
    // StoreAdapter interface

    @Override
    public GroupCursor newGroupCursor(GroupTable groupTable, boolean reverse, IndexKeyRange indexKeyRange)
    {
        GroupCursor cursor;
        try {
            cursor = new ModifiablePersistitGroupCursor(this, groupTable, reverse, indexKeyRange);
        } catch (PersistitException e) {
            throw new StoreAdapterRuntimeException(e);
        }
        return cursor;
    }

    @Override
    public Cursor newIndexCursor(Index index, boolean reverse, IndexKeyRange keyRange)
    {
        Cursor cursor;
        try {
            cursor = new PersistitIndexCursor(this, schema.indexRowType(index), reverse, keyRange);
        } catch (PersistitException e) {
            throw new StoreAdapterRuntimeException(e);
        }
        return cursor;
    }

    public void setTransactional(boolean transactional) {
        this.transactional.set(transactional);
    }

    // PersistitAdapter interface

    public RowDef rowDef(int tableId)
    {
        return persistit.getRowDefCache().getRowDef(tableId);
    }

    public PersistitGroupRow newGroupRow()
    {
        // TODO: Pool rows?
        return PersistitGroupRow.newPersistitGroupRow(this);
    }

    public PersistitIndexRow newIndexRow(IndexRowType indexRowType) throws PersistitException
    {
        // TODO: Pool rows?
        return new PersistitIndexRow(this, indexRowType);
    }

    public RowData rowData(RowDef rowDef, RowBase row) {
        if (row instanceof PersistitGroupRow) {
            return ((PersistitGroupRow)row).rowData();
        }
        
        NewRow niceRow = new NiceRow(rowDef.getRowDefId(), rowDef);

        for(int i=0; i < row.rowType().nFields(); ++i) {
            niceRow.put(i, row.field(i));
        }
        return niceRow.toRowData();
    }

    public Exchange takeExchange(GroupTable table) throws PersistitException
    {
        return transact(persistit.getExchange(session, (RowDef) table.rowDef(), null));
    }

    public Exchange takeExchange(Index index) throws PersistitException
    {
        return transact(persistit.getExchange(session, null, (IndexDef) index.indexDef()));
    }

    public void updateIndex(IndexDef indexDef, RowBase oldRow, RowBase newRow, Key hKey)
        throws PersistitAdapterException, DuplicateKeyException
    {
        RowDef rowDef = indexDef.getRowDef();
        RowData oldRowData = rowData(rowDef, oldRow);
        RowData newRowData = rowData(rowDef, newRow);
        try {
            persistit.updateIndex(session, indexDef, rowDef, oldRowData, newRowData, hKey);
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    private Exchange transact(Exchange exchange) {
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

    public void commitAllTransactions() throws PersistitException {
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

    public PersistitAdapter(Schema schema, PersistitStore persistit, Session session)
    {
        super(schema);
        this.persistit = persistit;
        this.session = session;
        this.filterFactory = new PersistitFilterFactory(this);
    }

    // Object state

    private final AtomicBoolean transactional = new AtomicBoolean(false);
    private final Map<Exchange,Transaction> transactionsMap = new HashMap<Exchange, Transaction>();
    final PersistitStore persistit;
    final Session session;
    final PersistitFilterFactory filterFactory;
}
