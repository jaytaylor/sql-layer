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
import com.akiban.qp.operator.*;
import com.akiban.qp.persistitadapter.indexcursor.IterationHelper;
import com.akiban.qp.persistitadapter.indexcursor.PersistitSorter;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRow;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRowPool;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.error.*;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.Store;
import com.akiban.server.types.ValueSource;
import com.akiban.util.tap.InOutTap;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InterruptedIOException;

public class PersistitAdapter extends StoreAdapter
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
    public Cursor newIndexCursor(QueryContext context, QueryBindings bindings, Index index, IndexKeyRange keyRange, API.Ordering ordering,
                                 IndexScanSelector selector, boolean usePValues)
    {
        return new PersistitIndexCursor(context,
                                        bindings,
                                        schema.indexRowType(index),
                                        keyRange,
                                        ordering,
                                        selector,
                                        usePValues);
    }

    @Override
    public Sorter createSorter(QueryContext context,
                               QueryBindings bindings,
                               Cursor input,
                               RowType rowType,
                               API.Ordering ordering,
                               API.SortOption sortOption,
                               InOutTap loadTap)
    {
        return new PersistitSorter(context, bindings, input, rowType, ordering, sortOption, loadTap);
    }

    @Override
    public HKey newHKey(com.akiban.ais.model.HKey hKeyMetadata)
    {
        return new PersistitHKey(store.createKey(), hKeyMetadata);
    }

    @Override
    public void updateRow(Row oldRow, Row newRow, boolean usePValues) {
        RowDef rowDef = oldRow.rowType().userTable().rowDef();
        RowDef rowDefNewRow = newRow.rowType().userTable().rowDef();
        if (rowDef.getRowDefId() != rowDefNewRow.getRowDefId()) {
            throw new IllegalArgumentException(String.format("%s != %s", rowDef, rowDefNewRow));
        }

        RowData oldRowData = rowData(rowDef, oldRow, rowDataCreator(usePValues));
        int oldStep = 0;
        try {
            // For Update row, the new row (value being inserted) does not
            // need the default value (including identity set)
            RowData newRowData = rowData(rowDefNewRow, newRow, rowDataCreator(usePValues));
            oldStep = enterUpdateStep();
            oldRowData.setExplicitRowDef(rowDef);
            newRowData.setExplicitRowDef(rowDefNewRow);
            store.updateRow(getSession(), oldRowData, newRowData, null, null);
        } catch (InvalidOperationException e) {
            rollbackIfNeeded(e);
            throw e;
        }
        finally {
            leaveUpdateStep(oldStep);
        }
    }
    @Override
    public void writeRow (Row newRow, boolean usePValues) {
        RowDef rowDef = newRow.rowType().userTable().rowDef();
        int oldStep = 0;
        try {
            RowData newRowData = rowData (rowDef, newRow, rowDataCreator(usePValues));
            oldStep = enterUpdateStep();
            store.writeRow(getSession(), newRowData);
        } catch (InvalidOperationException e) {
            rollbackIfNeeded(e);
            throw e;
        }
        finally {
            leaveUpdateStep(oldStep);
        }
    }

    @Override
    public void deleteRow (Row oldRow, boolean usePValues, boolean cascadeDelete) {
        RowDef rowDef = oldRow.rowType().userTable().rowDef();
        RowData oldRowData = rowData(rowDef, oldRow, rowDataCreator(usePValues));
        oldRowData.setExplicitRowDef(rowDef);
        int oldStep = enterUpdateStep();
        try {
            store.deleteRow(getSession(), oldRowData, true, cascadeDelete);
        } catch (InvalidOperationException e) {
            rollbackIfNeeded(e);
            throw e;
        }
        finally {
            leaveUpdateStep(oldStep);
        }
    }

    @Override
    public void alterRow(Row oldRow, Row newRow, Index[] indexes, boolean hKeyChanged, boolean usePValues) {
        RowDef rowDef = oldRow.rowType().userTable().rowDef();
        RowDef rowDefNewRow = newRow.rowType().userTable().rowDef();
        RowData oldRowData = rowData(rowDef, oldRow, rowDataCreator(usePValues));

        int oldStep = 0;
        try {
            // Altered row does not need defaults from newRowData()
            RowData newRowData = rowData(rowDefNewRow, newRow, rowDataCreator(usePValues));
            oldRowData.setExplicitRowDef(rowDef);
            newRowData.setExplicitRowDef(rowDefNewRow);
            if(hKeyChanged) {
                store.deleteRow(getSession(), oldRowData, false, false);
                oldStep = enterUpdateStep();
                store.writeRow(getSession(), newRowData);
            } else {
                oldStep = enterUpdateStep();
                store.updateRow(getSession(), oldRowData, newRowData, null, indexes);
            }
        } catch (InvalidOperationException e) {
            rollbackIfNeeded(e);
            throw e;
        }
        finally {
            leaveUpdateStep(oldStep);
        }
    }

    @Override
    public long rowCount(Session session, RowType tableType) {
        RowDef rowDef = tableType.userTable().rowDef();
        return rowDef.getTableStatus().getRowCount(session);
    }

    @Override
    public long hash(ValueSource valueSource, AkCollator collator)
    {
        assert collator != null; // Caller should have hashed in this case
        long hash;
        Key key;
        int depth;
        if (valueSource instanceof PersistitKeyValueSource) {
            PersistitKeyValueSource persistitKeyValueSource = (PersistitKeyValueSource) valueSource;
            key = persistitKeyValueSource.key();
            depth = persistitKeyValueSource.depth();
        } else {
            key = persistit.createKey();
            collator.append(key, valueSource.getString());
            depth = 0;
        }
        hash = keyHasher.hash(key, depth);
        return hash;
    }


    @Override
    public <S> RowData rowData(RowDef rowDef, RowBase row, RowDataCreator<S> creator) {
        if(row instanceof PersistitGroupRow) {
            return ((PersistitGroupRow)row).rowData();
        }
        return super.rowData(rowDef, row, creator);
    }

    // PersistitAdapter interface

    public PersistitStore persistit()
    {
        return persistit;
    }

    public RowDef rowDef(int tableId)
    {
        return schema.ais().getUserTable(tableId).rowDef();
    }

    private RowDataCreator<?> rowDataCreator(boolean usePValues) {
        return usePValues
                ? new PValueRowDataCreator()
                : new OldRowDataCreator();
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
        assert !indexRow.isShared();
        indexRowPool.returnIndexRow(this, indexRow.rowType(), indexRow);
    }

    @Override
    public IterationHelper createIterationHelper(IndexRowType indexRowType) {
        return new PersistitIterationHelper(this, indexRowType);
    }

    public Exchange takeExchange(Group group) throws PersistitException
    {
        return persistit.getExchange(getSession(), group);
    }

    public Exchange takeExchange(Index index)
    {
        return persistit.getExchange(getSession(), index);
    }

    public Key newKey()
    {
        return new Key(persistit.getDb());
    }

    public void handlePersistitException(PersistitException e)
    {
        handlePersistitException(getSession(), e);
    }

    public static boolean isFromInterruption(Exception e) {
        Throwable cause = e.getCause();
        return (e instanceof PersistitInterruptedException) ||
               ((cause != null) &&
                (cause instanceof PersistitInterruptedException ||
                 cause instanceof InterruptedIOException ||
                 cause instanceof InterruptedException));
    }

    public static RuntimeException wrapPersistitException(Session session, PersistitException e)
    {
        assert e != null;
        if (isFromInterruption(e)) {
            return new QueryCanceledException(session);
        } else {
            return new PersistitAdapterException(e);
        }
    }

    public static void handlePersistitException(Session session, PersistitException e)
    {
        throw wrapPersistitException(session, e);
    }

    public void returnExchange(Exchange exchange)
    {
        persistit.releaseExchange(getSession(), exchange);
    }

    public Transaction transaction() {
        return treeService.getTransaction(getSession());
    }

    public void withStepChanging(boolean withStepChanging) {
        this.withStepChanging = withStepChanging;
    }

    @Override
    public int enterUpdateStep()
    {
        return enterUpdateStep(false);
    }

    @Override
    public int enterUpdateStep(boolean evenIfZero)
    {
        Transaction transaction = transaction();
        int step = transaction.getStep();
        if ((evenIfZero || step > 0) && withStepChanging)
            transaction.incrementStep();
        return step;
    }

    @Override
    public void leaveUpdateStep(int step) {
        Transaction txn = transaction();
        if(txn.isActive() && !txn.isRollbackPending()) {
            txn.setStep(step);
        }
    }

    public PersistitAdapter(Schema schema,
                            Store store,
                            TreeService treeService,
                            Session session,
                            ConfigurationService config)
    {
        this(schema, store, treeService, session, config, true);
    }

    public PersistitAdapter(Schema schema,
                            Store store,
                            TreeService treeService,
                            Session session,
                            ConfigurationService config,
                            boolean withStepChanging)
    {
        super(schema, session, config);
        this.store = store;
        this.persistit = store.getPersistitStore();
        assert this.persistit != null : store;
        this.treeService = treeService;
        this.withStepChanging = withStepChanging;
        session.put(STORE_ADAPTER_KEY, this);
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
    public long sequenceNextValue(TableName sequenceName) {
        Sequence sequence = schema().ais().getSequence(sequenceName);
        if (sequence == null) {
            throw new NoSuchSequenceException (sequenceName);
        }
        return sequenceValue (sequence, false);
    }

    @Override
    public long sequenceCurrentValue(TableName sequenceName) {
        Sequence sequence = schema().ais().getSequence(sequenceName);
        if (sequence == null) {
            throw new NoSuchSequenceException (sequenceName);
        }
        return sequenceValue (sequence, true);
    }

    @Override
    public Key createKey() {
        return store.createKey();
    }

    private long sequenceValue (Sequence sequence, boolean getCurrentValue) {
        try {
            if (getCurrentValue) {
                return sequence.currentValue();
            } else {
                return sequence.nextValue();
            }
        } catch (PersistitException e) {
            rollbackIfNeeded(e);
            handlePersistitException(e);
            assert false;
            return 0;
        }
    }

    // Class state

    private static PersistitIndexRowPool indexRowPool = new PersistitIndexRowPool();

    // Object state

    private final TreeService treeService;
    private final Store store;
    private final PersistitStore persistit;
    private boolean withStepChanging;
    private final PersistitKeyHasher keyHasher = new PersistitKeyHasher();
}
