/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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
import com.foundationdb.qp.operator.API.Ordering;
import com.foundationdb.qp.operator.API.SortOption;
import com.foundationdb.qp.operator.GroupCursor;
import com.foundationdb.qp.operator.IndexScanSelector;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.storeadapter.indexcursor.IterationHelper;
import com.foundationdb.qp.storeadapter.indexcursor.MergeJoinSorter;
import com.foundationdb.qp.storeadapter.indexrow.IndexRowPool;
import com.foundationdb.qp.storeadapter.indexrow.MemoryIndexRow;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.store.MemoryStore;
import com.foundationdb.util.tap.InOutTap;

import java.util.Collection;

public class MemoryAdapter extends StoreAdapter
{
    private static final IndexRowPool indexRowPool = new IndexRowPool();

    private final MemoryStore store;

    public MemoryAdapter(Session session, ConfigurationService config, MemoryStore store) {
        super(session, config);
        this.store = store;
    }


    @Override
    public GroupCursor newGroupCursor(Group group) {
        return new MemoryGroupCursor(this, group);
    }

    @Override
    public RowCursor newIndexCursor(QueryContext context,
                                    IndexRowType rowType,
                                    IndexKeyRange keyRange,
                                    Ordering ordering,
                                    IndexScanSelector scanSelector,
                                    boolean openAllSubCursors) {
        return new StoreAdapterIndexCursor(context,
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
        } catch(DuplicateKeyException e) {
            store.setRollbackPending(getSession());
            throw e;
        }
    }

    @Override
    public void writeRow(Row newRow, Collection<TableIndex> tableIndexes, Collection<GroupIndex> groupIndexes) {
        try {
            store.writeRow(getSession(), newRow, tableIndexes, groupIndexes);
        } catch(DuplicateKeyException e) {
            store.setRollbackPending(getSession());
            throw e;
        }
    }

    @Override
    public void deleteRow(Row oldRow, boolean cascadeDelete) {
        try {
            store.deleteRow(getSession(), oldRow, cascadeDelete);
        } catch(DuplicateKeyException e) {
            store.setRollbackPending(getSession());
            throw e;
        }
    }

    @Override
    public Sorter createSorter(QueryContext context,
                               QueryBindings bindings,
                               RowCursor input,
                               RowType rowType,
                               Ordering ordering,
                               SortOption sortOption,
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
    public IndexRow newIndexRow(IndexRowType indexRowType) {
        return new MemoryIndexRow(getUnderlyingStore(), indexRowType);
    }

    @Override
    public IndexRow takeIndexRow(IndexRowType indexRowType) {
        return indexRowPool.takeIndexRow(this, indexRowType);
    }

    @Override
    public void returnIndexRow(IndexRow indexRow) {
        indexRowPool.returnIndexRow(this, indexRow.rowType(), indexRow);
    }

    @Override
    public IterationHelper createIterationHelper(IndexRowType indexRowType) {
        return new MemoryIterationHelper(this, indexRowType);
    }

    @Override
    public KeyCreator getKeyCreator() {
        return store;
    }

    @Override
    protected MemoryStore getUnderlyingStore() {
        return store;
    }

    @Override
    public AkibanInformationSchema getAIS() {
        return store.getAIS(getSession());
    }
}
