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

package com.foundationdb.qp.persistitadapter;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.IndexScanSelector;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.persistitadapter.indexcursor.IterationHelper;
import com.foundationdb.qp.persistitadapter.indexcursor.MergeJoinSorter;
import com.foundationdb.qp.persistitadapter.indexrow.PersistitIndexRow;
import com.foundationdb.qp.persistitadapter.indexrow.PersistitIndexRowPool;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.error.FDBAdapterException;
import com.foundationdb.server.error.FDBCommitUnknownResultException;
import com.foundationdb.server.error.FDBNotCommittedException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.QueryCanceledException;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.FDBStore;
import com.foundationdb.server.store.FDBTransactionService;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.FDBException;
import com.persistit.Key;

import java.io.InterruptedIOException;

public class FDBAdapter extends StoreAdapter {
    private static final PersistitIndexRowPool indexRowPool = new PersistitIndexRowPool();

    private final FDBStore store;
    private final FDBTransactionService txnService;

    public FDBAdapter(FDBStore store, Schema schema, Session session, FDBTransactionService txnService, ConfigurationService config) {
        super(schema, session, config);
        this.store = store;
        this.txnService = txnService;
    }

    @Override
    public FDBGroupCursor newGroupCursor(Group group) {
        return new FDBGroupCursor(this, group);
    }

    @Override
    public FDBGroupCursor newDumpGroupCursor(Group group, int commitFrequency) {
        return new FDBGroupCursor(this, group, commitFrequency);
    }

    @Override
    public RowCursor newIndexCursor(QueryContext context,
                                    Index index,
                                    IndexKeyRange keyRange,
                                    API.Ordering ordering,
                                    IndexScanSelector scanSelector,
                                    boolean openAllSubCursors) {
        return new PersistitIndexCursor(context,
                                        schema.indexRowType(index),
                                        keyRange,
                                        ordering,
                                        scanSelector,
                                        openAllSubCursors);
    }

    @Override
    public PersistitHKey newHKey(com.foundationdb.ais.model.HKey hKeyMetadata) {
        return new PersistitHKey(store.createKey(), hKeyMetadata);
    }

    @Override
    public void updateRow(Row oldRow, Row newRow) {
        RowDef rowDef = newRow.rowType().userTable().rowDef();
        RowData oldRowData = rowData(rowDef, oldRow, new ValueRowDataCreator());
        RowData newRowData = rowData(rowDef, newRow, new ValueRowDataCreator());
        oldRowData.setExplicitRowDef(rowDef);
        newRowData.setExplicitRowDef(rowDef);
        try {
            store.updateRow(getSession(), oldRowData, newRowData, null);
        } catch(InvalidOperationException e) {
            rollbackIfNeeded(getSession(), e);
            throw e;
        }
    }

    @Override
    public void writeRow(Row newRow, Index[] indexes) {
        RowDef rowDef = newRow.rowType().userTable().rowDef();
        RowData newRowData = rowData(rowDef, newRow, new ValueRowDataCreator());
        try {
            store.writeRow(getSession(), newRowData, indexes);
        } catch(InvalidOperationException e) {
            rollbackIfNeeded(getSession(), e);
            throw e;
        }
    }

    @Override
    public void deleteRow(Row oldRow, boolean cascadeDelete) {
        RowDef rowDef = oldRow.rowType().userTable().rowDef();
        RowData oldRowData = rowData(rowDef, oldRow, new ValueRowDataCreator());
        try {
            store.deleteRow(getSession(), oldRowData, true, cascadeDelete);
        } catch(InvalidOperationException e) {
            rollbackIfNeeded(getSession(), e);
            throw e;
        }
    }

    @Override
    public Sorter createSorter(QueryContext context,
                               QueryBindings bindings,
                               RowCursor input,
                               RowType rowType,
                               API.Ordering ordering,
                               API.SortOption sortOption,
                               InOutTap loadTap) {
        return new MergeJoinSorter(context, bindings, input, rowType, ordering, sortOption, loadTap);
    }

    @Override
    public long sequenceNextValue(TableName sequenceName) {
        return store.nextSequenceValue(getSession(), store.getAIS(getSession()).getSequence(sequenceName));
    }

    @Override
    public long sequenceCurrentValue(TableName sequenceName) {
        return store.curSequenceValue(getSession(), store.getAIS(getSession()).getSequence(sequenceName));
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
        return new FDBIterationHelper(this, indexRowType);
    }

    @Override
    protected FDBStore getUnderlyingStore() {
        return store;
    }

    @Override
    public Key createKey() {
        return store.createKey();
    }


    public FDBTransactionService.TransactionState getTransaction() {
        return txnService.getTransaction(getSession());
    }

    //
    // Internal
    //

    public static boolean isFromInterruption(Exception e) {
        Throwable c = e.getCause();
        // TODO: Is the IO needed?
        return (e instanceof InterruptedException) || (e instanceof InterruptedIOException) ||
               (c instanceof InterruptedException) || (c instanceof InterruptedIOException);
    }

    public static RuntimeException wrapFDBException(Session session, Exception e)
    {
        if (isFromInterruption(e)) {
            return new QueryCanceledException(session);
        } else if (e instanceof FDBException) {
            switch (((FDBException)e).getCode()) {
            case 1020:          // not_committed
                return new FDBNotCommittedException(e);
            case 1021:          // commit_unknown_result
                return new FDBCommitUnknownResultException(e);
            default:
                return new FDBAdapterException(e);
            }
        } else if (e instanceof RuntimeException) {
            return (RuntimeException)e;
        } else {
            return new AkibanInternalException("unexpected error from data layer", e);
        }
    }

    private void rollbackIfNeeded(Session session, Exception e) {
        if((e instanceof DuplicateKeyException) || (e instanceof FDBException) || isFromInterruption(e)) {
            store.setRollbackPending(session);
        }
    }
}
