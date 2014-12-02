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

package com.foundationdb.qp.storeadapter;

import com.foundationdb.ais.model.*;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.*;
import com.foundationdb.qp.storeadapter.indexcursor.IterationHelper;
import com.foundationdb.qp.storeadapter.indexcursor.MergeJoinSorter;
import com.foundationdb.qp.storeadapter.indexrow.IndexRowPool;
import com.foundationdb.qp.storeadapter.indexrow.PersistitGroupIndexRow;
import com.foundationdb.qp.storeadapter.indexrow.PersistitTableIndexRow;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.*;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.service.tree.TreeService;
import com.foundationdb.server.store.PersistitStore;
import com.foundationdb.server.store.Store;
import com.foundationdb.util.tap.InOutTap;
import com.persistit.Exchange;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.RollbackException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InterruptedIOException;
import java.util.Collection;

public class PersistitAdapter extends StoreAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(PersistitAdapter.class);
    // StoreAdapter interface

    @Override
    public AkibanInformationSchema getAIS() {
        return getUnderlyingStore().getAIS(getSession());
    }

    @Override
    public GroupCursor newGroupCursor(Group group)
    {
        GroupCursor cursor;
        try {
            cursor = new PersistitGroupCursor(this, group);
        } catch (PersistitException e) {
            handlePersistitException(e);
            throw new AssertionError();
        }
        return cursor;
    }

    @Override
    public RowCursor newIndexCursor(QueryContext context,
                                    IndexRowType rowType,
                                    IndexKeyRange keyRange, 
                                    API.Ordering ordering,
                                    IndexScanSelector scanSelector,
                                    boolean openAllSubCursors) {
        return new PersistitIndexCursor(context,
                rowType,
                keyRange,
                ordering,
                scanSelector,
                openAllSubCursors);
    }

    @Override
    public Sorter createSorter(QueryContext context,
                               QueryBindings bindings,
                               RowCursor input,
                               RowType rowType,
                               API.Ordering ordering,
                               API.SortOption sortOption,
                               InOutTap loadTap)
    {
        return new MergeJoinSorter(context, bindings, input, rowType, ordering, sortOption, loadTap);
    }


    @Override
    public void updateRow(Row oldRow, Row newRow) {
        try {
            store.updateRow(getSession(), oldRow, newRow);
        } catch (InvalidOperationException e) {
            rollbackIfNeeded(e);
            throw e;
        }
    }
    @Override
    public void writeRow(Row newRow, Collection<TableIndex> indexes, Collection<GroupIndex> groupIndexes) {
        try {
            store.writeRow(getSession(), newRow, indexes, groupIndexes);
        } catch (InvalidOperationException e) {
            rollbackIfNeeded(e);
            throw e;
        }
    }

    @Override
    public void deleteRow (Row oldRow, boolean cascadeDelete) {
        try {
            store.deleteRow(getSession(), oldRow, cascadeDelete);
        } catch (InvalidOperationException e) {
            rollbackIfNeeded(e);
            throw e;
        }
    }

    // PersistitAdapter interface

    public PersistitStore persistit()
    {
        return store;
    }

    @Override
    public IndexRow newIndexRow(IndexRowType indexRowType) 
    {
        return
                indexRowType.index().isTableIndex()
                ? new PersistitTableIndexRow(this, indexRowType)
                : new PersistitGroupIndexRow(this, indexRowType);
     
    }
    @Override
    public IndexRow takeIndexRow(IndexRowType indexRowType)
    {
        return indexRowPool.takeIndexRow(this, indexRowType);
    }

    @Override
    public void returnIndexRow(IndexRow indexRow)
    {
        indexRowPool.returnIndexRow(this, indexRow.rowType(), indexRow);
    }

    @Override
    public IterationHelper createIterationHelper(IndexRowType indexRowType) {
        return new PersistitIterationHelper(this, indexRowType);
    }

    @Override
    public KeyCreator getKeyCreator() {
        return store;
    }

    public Exchange takeExchange(Group group) throws PersistitException
    {
        return store.getExchange(getSession(), group);
    }

    public Exchange takeExchange(Index index)
    {
        return store.getExchange(getSession(), index);
    }

    public void handlePersistitException(PersistitException e)
    {
        handlePersistitException(getSession(), e);
    }

    public static boolean isFromInterruption(Throwable e) {
        Throwable cause = e.getCause();
        return (e instanceof PersistitInterruptedException) ||
               ((cause != null) &&
                (cause instanceof PersistitInterruptedException ||
                 cause instanceof InterruptedIOException ||
                 cause instanceof InterruptedException));
    }

    public static RuntimeException wrapPersistitException(Session session, Throwable e)
    {
        assert e != null;
        if (isFromInterruption(e)) {
            return new QueryCanceledException(session);
        } else if (e instanceof RollbackException) {
            return new PersistitRollbackException(e);
        } else {
            return new PersistitAdapterException(e);
        }
    }

    public static void handlePersistitException(Session session, Throwable e)
    {
        throw wrapPersistitException(session, e);
    }

    public void returnExchange(Exchange exchange)
    {
        store.releaseExchange(getSession(), exchange);
    }

    private Transaction transaction() {
        return treeService.getTransaction(getSession());
    }

    public PersistitAdapter(PersistitStore store,
                            TreeService treeService,
                            Session session,
                            ConfigurationService config)
    {
        super(session, config);
        this.store = store;
        this.treeService = treeService;
    }

    // For use within hierarchy

    @Override
    protected Store getUnderlyingStore() {
        return store;
    }

    // For use by this class
    private void rollbackIfNeeded(Exception e) {
        if((e instanceof DuplicateKeyException) ||
           (e instanceof PersistitException) ||
           (e instanceof PersistitAdapterException) ||
           isFromInterruption(e)) {
            Transaction txn = transaction();
            if(txn.isActive()) {
                txn.rollback();
            }
        }
    }

    @Override
    public long sequenceNextValue(Sequence sequence) {
        return store.nextSequenceValue(getSession(), sequence);
    }

    @Override
    public long sequenceCurrentValue(Sequence sequence) {
        return store.curSequenceValue(getSession(), sequence);
    }

    // Class state

    private static IndexRowPool indexRowPool = new IndexRowPool();

    // Object state

    private final TreeService treeService;
    private final PersistitStore store;

}
