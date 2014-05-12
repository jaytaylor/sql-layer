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

package com.foundationdb.qp.memoryadapter;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.GroupCursor;
import com.foundationdb.qp.operator.IndexScanSelector;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.operator.API.Ordering;
import com.foundationdb.qp.operator.API.SortOption;
import com.foundationdb.qp.storeadapter.Sorter;
import com.foundationdb.qp.storeadapter.indexcursor.IterationHelper;
import com.foundationdb.qp.storeadapter.indexrow.PersistitIndexRow;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.format.MemoryTableStorageDescription;
import com.foundationdb.util.tap.InOutTap;
import com.persistit.Key;

import java.util.Collection;

public class MemoryAdapter extends StoreAdapter {

    public MemoryAdapter(Schema schema, 
            Session session,
            ConfigurationService config) {
        super(schema, session, config);
    }

    public static MemoryTableFactory getMemoryTableFactory(Table table) {
        // NOTE: This assumes that a memory table group never has more
        // than one table or at least that they all have equivalent
        // factories.
        return getMemoryTableFactory(table.getGroup());
    }

    public static MemoryTableFactory getMemoryTableFactory(Group group) {
        return ((MemoryTableStorageDescription)group.getStorageDescription())
            .getMemoryTableFactory();
    }

    @Override
    public GroupCursor newGroupCursor(Group group) {
        return new MemoryGroupCursor(this, group);
    }

    @Override
    public <HKEY extends HKey> HKEY newHKey(
            com.foundationdb.ais.model.HKey hKeyMetadata) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Store getUnderlyingStore() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowCursor newIndexCursor(QueryContext context, Index index,
            IndexKeyRange keyRange, Ordering ordering,
            IndexScanSelector scanSelector, boolean openAllSubCursors) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Sorter createSorter(QueryContext context, QueryBindings bindings, RowCursor input, RowType rowType,
                               Ordering ordering, SortOption sortOption, InOutTap loadTap) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRow(Row oldRow, Row newRow) {
        throw new UnsupportedOperationException();
        
    }

    @Override
    public void writeRow(Row newRow, TableIndex[] indexes, Collection<GroupIndex> groupIndexes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteRow(Row oldRow, boolean cascadeDelete) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sequenceNextValue(Sequence sequence) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sequenceCurrentValue(Sequence sequence) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PersistitIndexRow takeIndexRow(IndexRowType indexRowType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void returnIndexRow(PersistitIndexRow indexRow) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IterationHelper createIterationHelper(IndexRowType indexRowType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Key createKey() {
        throw new UnsupportedOperationException();
    }
}
