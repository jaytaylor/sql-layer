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
import com.foundationdb.qp.persistitadapter.indexcursor.MemorySorter;
import com.foundationdb.qp.persistitadapter.indexrow.PersistitIndexRow;
import com.foundationdb.qp.persistitadapter.indexrow.PersistitIndexRowPool;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.PersistitKeyValueSource;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.FDBStore;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.FDBException;
import com.persistit.Key;

import java.io.InterruptedIOException;

public class FDBAdapter extends StoreAdapter {
    private static final PersistitIndexRowPool indexRowPool = new PersistitIndexRowPool();

    private final FDBStore store;
    private final PersistitKeyHasher keyHasher = new PersistitKeyHasher();

    public FDBAdapter(FDBStore store, Schema schema, Session session, ConfigurationService config) {
        super(schema, session, config);
        this.store = store;
    }

    @Override
    public FDBGroupCursor newGroupCursor(Group group) {
        return new FDBGroupCursor(this, group);
    }

    @Override
    public RowCursor newIndexCursor(QueryContext context,
                                    Index index,
                                    IndexKeyRange keyRange,
                                    API.Ordering ordering,
                                    IndexScanSelector scanSelector,
                                    boolean usePValues,
                                    boolean openAllSubCursors) {
        return new PersistitIndexCursor(context,
                                        schema.indexRowType(index),
                                        keyRange,
                                        ordering,
                                        scanSelector,
                                        usePValues,
                                        openAllSubCursors);
    }

    @Override
    public PersistitHKey newHKey(com.foundationdb.ais.model.HKey hKeyMetadata) {
        return new PersistitHKey(store.createKey(), hKeyMetadata);
    }

    @Override
    public void updateRow(Row oldRow, Row newRow, boolean usePValues) {
        RowDef rowDef = newRow.rowType().userTable().rowDef();
        RowData oldRowData = rowData(rowDef, oldRow, new PValueRowDataCreator());
        RowData newRowData = rowData(rowDef, newRow, new PValueRowDataCreator());
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
    public void writeRow(Row newRow, Index[] indexes, boolean usePValues) {
        RowDef rowDef = newRow.rowType().userTable().rowDef();
        RowData newRowData = rowData(rowDef, newRow, new PValueRowDataCreator());
        try {
            store.writeRow(getSession(), newRowData, indexes);
        } catch(InvalidOperationException e) {
            rollbackIfNeeded(getSession(), e);
            throw e;
        }
    }

    @Override
    public void deleteRow(Row oldRow, boolean usePValues, boolean cascadeDelete) {
        RowDef rowDef = oldRow.rowType().userTable().rowDef();
        RowData oldRowData = rowData(rowDef, oldRow, new PValueRowDataCreator());
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
        return new MemorySorter(context, bindings, input, rowType, ordering, sortOption, loadTap, store.createKey());
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
    public long hash(ValueSource valueSource, AkCollator collator) {
        assert collator != null; // Caller should have hashed in this case
        long hash;
        Key key;
        int depth;
        if (valueSource instanceof PersistitKeyValueSource) {
            PersistitKeyValueSource persistitKeyValueSource = (PersistitKeyValueSource) valueSource;
            key = persistitKeyValueSource.key();
            depth = persistitKeyValueSource.depth();
        } else {
            key = createKey();
            collator.append(key, valueSource.getString());
            depth = 0;
        }
        hash = keyHasher.hash(key, depth);
        return hash;
    }

    @Override
    public int enterUpdateStep() {
        // TODO
        return 0;
    }

    @Override
    public int enterUpdateStep(boolean evenIfZero) {
        // TODO
        return 0;
    }

    @Override
    public void leaveUpdateStep(int step) {
        // TODO
    }

    @Override
    public void withStepChanging(boolean withStepChanging) {
        // TODO
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


    //
    // Internal
    //

    public static boolean isFromInterruption(Exception e) {
        Throwable c = e.getCause();
        // TODO: Is the IO needed?
        return (e instanceof InterruptedException) || (e instanceof InterruptedIOException) ||
               (c instanceof InterruptedException) || (c instanceof InterruptedIOException);
    }

    private void rollbackIfNeeded(Session session, Exception e) {
        if((e instanceof DuplicateKeyException) || (e instanceof FDBException) || isFromInterruption(e)) {
            store.setRollbackPending(session);
        }
    }
}
