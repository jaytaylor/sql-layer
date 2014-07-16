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
import com.foundationdb.qp.storeadapter.indexrow.PersistitIndexRow;
import com.foundationdb.qp.storeadapter.indexrow.PersistitIndexRowPool;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.error.*;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.service.tree.TreeService;
import com.foundationdb.server.store.PersistitStore;
import com.foundationdb.server.store.Store;
import com.foundationdb.util.tap.InOutTap;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.RollbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InterruptedIOException;
import java.util.Collection;

public class PersistitAdapter extends StoreAdapter implements KeyCreator
{
    private static final Logger logger = LoggerFactory.getLogger(PersistitAdapter.class);
    // StoreAdapter interface

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
    public RowCursor newIndexCursor(QueryContext context, Index index, IndexKeyRange keyRange, API.Ordering ordering,
                                    IndexScanSelector selector, boolean openAllSubCursors)
    {
        return new PersistitIndexCursor(context,
                                        schema.indexRowType(index),
                                        keyRange,
                                        ordering,
                                        selector,
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
        //return new PersistitSorter(context, bindings, input, rowType, ordering, sortOption, loadTap);
        return new MergeJoinSorter(context, bindings, input, rowType, ordering, sortOption, loadTap);
    }

    @Override
    public HKey newHKey(com.foundationdb.ais.model.HKey hKeyMetadata)
    {
        return new PersistitHKey(store.createKey(), hKeyMetadata);
    }

    @Override
    public void updateRow(Row oldRow, Row newRow) {
        RowDef rowDef = oldRow.rowType().table().rowDef();
        RowDef rowDefNewRow = newRow.rowType().table().rowDef();
        if (rowDef.getRowDefId() != rowDefNewRow.getRowDefId()) {
            throw new IllegalArgumentException(String.format("%s != %s", rowDef, rowDefNewRow));
        }

        RowData oldRowData = rowData(rowDef, oldRow, rowDataCreator());
        try {
            // For Update row, the new row (value being inserted) does not
            // need the default value (including identity set)
            RowData newRowData = rowData(rowDefNewRow, newRow, rowDataCreator());
            store.updateRow(getSession(), rowDef, oldRowData, rowDefNewRow, newRowData, null);
        } catch (InvalidOperationException e) {
            rollbackIfNeeded(e);
            throw e;
        }
    }
    @Override
    public void writeRow(Row newRow, TableIndex[] indexes, Collection<GroupIndex> groupIndexes) {
        RowDef rowDef = newRow.rowType().table().rowDef();
        try {
            RowData newRowData = rowData (rowDef, newRow, rowDataCreator());
            store.writeRow(getSession(), rowDef, newRowData, indexes, groupIndexes);
        } catch (InvalidOperationException e) {
            rollbackIfNeeded(e);
            throw e;
        }
    }

    @Override
    public void deleteRow (Row oldRow, boolean cascadeDelete) {
        RowDef rowDef = oldRow.rowType().table().rowDef();
        RowData oldRowData = rowData(rowDef, oldRow, rowDataCreator());
        try {
            store.deleteRow(getSession(), rowDef, oldRowData, cascadeDelete);
        } catch (InvalidOperationException e) {
            rollbackIfNeeded(e);
            throw e;
        }
    }

    @Override
    public RowData rowData(RowDef rowDef, Row row, RowDataCreator creator) {
        if(row instanceof PersistitGroupRow) {
            return ((PersistitGroupRow)row).rowData();
        }
        return super.rowData(rowDef, row, creator);
    }

    // PersistitAdapter interface

    public PersistitStore persistit()
    {
        return store;
    }

    public RowDef rowDef(int tableId)
    {
        return schema.ais().getTable(tableId).rowDef();
    }

    private RowDataCreator rowDataCreator() {
        return new RowDataCreator();
    }

    public PersistitGroupRow newGroupRow()
    {
        return PersistitGroupRow.newPersistitGroupRow(this);
    }

    @Override
    public PersistitIndexRow takeIndexRow(IndexRowType indexRowType)
    {
        return indexRowPool.takeIndexRow(this, indexRowType);
    }

    @Override
    public void returnIndexRow(PersistitIndexRow indexRow)
    {
        indexRowPool.returnIndexRow(this, indexRow.rowType(), indexRow);
    }

    @Override
    public IterationHelper createIterationHelper(IndexRowType indexRowType) {
        return new PersistitIterationHelper(this, indexRowType);
    }

    public Exchange takeExchange(Group group) throws PersistitException
    {
        return store.getExchange(getSession(), group);
    }

    public Exchange takeExchange(Index index)
    {
        return store.getExchange(getSession(), index);
    }

    public Key newKey()
    {
        return new Key(store.getDb());
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

    public PersistitAdapter(Schema schema,
                            PersistitStore store,
                            TreeService treeService,
                            Session session,
                            ConfigurationService config)
    {
        super(schema, session, config);
        this.store = store;
        this.treeService = treeService;
    }

    // For use within hierarchy

    @Override
    public Store getUnderlyingStore() {
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

    @Override
    public Key createKey() {
        return store.createKey();
    }


    // Class state

    private static PersistitIndexRowPool indexRowPool = new PersistitIndexRowPool();

    // Object state

    private final TreeService treeService;
    private final PersistitStore store;
}
