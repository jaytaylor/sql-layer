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

import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.IndexScanSelector;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.indexcursor.IterationHelper;
import com.akiban.qp.persistitadapter.indexcursor.MemorySorter;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.FDBStore;
import com.akiban.server.types.ValueSource;
import com.akiban.util.tap.InOutTap;
import com.persistit.Key;

public class FDBAdapter extends StoreAdapter {
    private final FDBStore store;

    public FDBAdapter(FDBStore store, Schema schema, Session session, ConfigurationService config) {
        super(schema, session, config);
        this.store = store;
    }

    @Override
    public FDBGroupCursor newGroupCursor(Group group) {
        return new FDBGroupCursor(this, group);
    }

    @Override
    public Cursor newIndexCursor(QueryContext context,
                                 Index index,
                                 IndexKeyRange keyRange,
                                 API.Ordering ordering,
                                 IndexScanSelector scanSelector,
                                 boolean usePValues) {
        return new FDBIndexCursor(this,
                                  context,
                                  schema.indexRowType(index),
                                  keyRange,
                                  ordering,
                                  scanSelector);
    }

    @Override
    public PersistitHKey newHKey(com.akiban.ais.model.HKey hKeyMetadata) {
        return new PersistitHKey(store.createKey(), hKeyMetadata);
    }

    @Override
    public void updateRow(Row oldRow, Row newRow, boolean usePValues) {
        RowDef rowDef = newRow.rowType().userTable().rowDef();
        RowData oldRowData = rowData(rowDef, oldRow, new PValueRowDataCreator());
        RowData newRowData = rowData(rowDef, newRow, new PValueRowDataCreator());
        oldRowData.setExplicitRowDef(rowDef);
        newRowData.setExplicitRowDef(rowDef);
        store.updateRow(getSession(), oldRowData, newRowData, null, null);
    }

    @Override
    public void writeRow(Row newRow, boolean usePValues) {
        RowDef rowDef = newRow.rowType().userTable().rowDef();
        RowData newRowData = rowData(rowDef, newRow, new PValueRowDataCreator());
        store.writeRow(getSession(), newRowData);
    }

    @Override
    public void deleteRow(Row oldRow, boolean usePValues, boolean cascadeDelete) {
        RowDef rowDef = oldRow.rowType().userTable().rowDef();
        RowData oldRowData = rowData(rowDef, oldRow, new PValueRowDataCreator());
        store.deleteRow(getSession(), oldRowData, true, cascadeDelete);
    }

    @Override
    public void alterRow(Row oldRow, Row newRow, Index[] indexesToMaintain, boolean hKeyChanged, boolean usePValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Sorter createSorter(QueryContext context,
                               Cursor input,
                               RowType rowType,
                               API.Ordering ordering,
                               API.SortOption sortOption,
                               InOutTap loadTap) {
        return new MemorySorter(context, input, rowType, ordering, sortOption, loadTap,
                                store.createKey());
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
        throw new UnsupportedOperationException();
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
    public PersistitIndexRow takeIndexRow(IndexRowType indexRowType) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public void returnIndexRow(PersistitIndexRow indexRow) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public IterationHelper createIterationHelper(IndexRowType indexRowType) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    protected FDBStore getUnderlyingStore() {
        return store;
    }

    @Override
    public Key createKey() {
        return store.createKey();
    }
}
