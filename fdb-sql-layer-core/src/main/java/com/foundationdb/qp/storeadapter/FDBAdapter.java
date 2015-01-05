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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.IndexScanSelector;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.storeadapter.indexcursor.IterationHelper;
import com.foundationdb.qp.storeadapter.indexcursor.MergeJoinSorter;
import com.foundationdb.qp.storeadapter.indexrow.IndexRowPool;
import com.foundationdb.qp.storeadapter.indexrow.FDBIndexRow;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.error.FDBAdapterException;
import com.foundationdb.server.error.FDBCommitUnknownResultException;
import com.foundationdb.server.error.FDBFutureVersionException;
import com.foundationdb.server.error.FDBNotCommittedException;
import com.foundationdb.server.error.FDBPastVersionException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.QueryCanceledException;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.store.FDBScanTransactionOptions;
import com.foundationdb.server.store.FDBStore;
import com.foundationdb.server.store.FDBTransactionService;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.FDBException;

import java.io.InterruptedIOException;
import java.util.Collection;

public class FDBAdapter extends StoreAdapter {
    private static final IndexRowPool indexRowPool = new IndexRowPool();

    private final FDBStore store;
    private final FDBTransactionService txnService;

    public FDBAdapter(FDBStore store, Session session, FDBTransactionService txnService, ConfigurationService config) {
        super(session, config);
        this.store = store;
        this.txnService = txnService;
    }

    @Override
    public FDBGroupCursor newGroupCursor(Group group) {
        return new FDBGroupCursor(this, group, scanOptions());
    }

    /** The transaction scan options for normal operator scans. */
    public FDBScanTransactionOptions scanOptions() {
        if (txnService.isTransactionActive(getSession()))
            return getTransaction().getScanOptions();
        // This should only happen during tests that aren't careful
        // about transactions, so it does not really matter.
        return FDBScanTransactionOptions.NORMAL;
    }

    @Override
    public FDBGroupCursor newDumpGroupCursor(Group group, int commitFrequency) {
        FDBScanTransactionOptions transactionOptions;
        if (commitFrequency == 0) {
            transactionOptions = FDBScanTransactionOptions.NORMAL;
        }
        else if (commitFrequency == StoreAdapter.COMMIT_FREQUENCY_PERIODICALLY) {
            transactionOptions = getTransaction().periodicallyCommitScanOptions();
        }
        else {
            transactionOptions = new FDBScanTransactionOptions(commitFrequency, -1);
        }
        return new FDBGroupCursor(this, group, transactionOptions);
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
    public void updateRow(Row oldRow, Row newRow) {
        try {
            store.updateRow(getSession(), oldRow, newRow);
        } catch(InvalidOperationException e) {
            rollbackIfNeeded(getSession(), e);
            throw e;
        }
    }

    @Override
    public void writeRow(Row newRow, Collection<TableIndex> indexes, Collection<GroupIndex> groupIndexes) {
        try {
            store.writeRow(getSession(), newRow, indexes, groupIndexes);
        } catch(InvalidOperationException e) {
            rollbackIfNeeded(getSession(), e);
            throw e;
        }
    }

    @Override
    public void deleteRow(Row oldRow, boolean cascadeDelete) {
        try {
            store.deleteRow(getSession(), oldRow, cascadeDelete);
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
    public long sequenceNextValue(Sequence sequence) {
        return store.nextSequenceValue(getSession(), sequence);
    }

    @Override
    public long sequenceCurrentValue(Sequence sequence) {
        return store.curSequenceValue(getSession(), sequence);
    }

    @Override
    public IndexRow newIndexRow(IndexRowType indexRowType)
    {
        return new FDBIndexRow (this.store, indexRowType);
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
        return new FDBIterationHelper(this, indexRowType);
    }
    
    @Override
    public KeyCreator getKeyCreator() {
        return store;
    }
    
    @Override
    public AkibanInformationSchema getAIS() {
        return store.getAIS(getSession());
    }

    @Override
    protected FDBStore getUnderlyingStore() {
        return store;
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
            FDBException fdbEx = (FDBException)e;
            switch (fdbEx.getCode()) {
            case 1007:          // past_version
                return new FDBPastVersionException(fdbEx);
            case 1009:          // future_version
                return new FDBFutureVersionException(fdbEx);
            case 1020:          // not_committed
                return new FDBNotCommittedException(fdbEx);
            case 1021:          // commit_unknown_result
                return new FDBCommitUnknownResultException(fdbEx);
            default:
                return new FDBAdapterException(fdbEx);
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
