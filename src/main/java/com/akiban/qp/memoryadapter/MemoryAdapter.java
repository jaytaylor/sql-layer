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

package com.akiban.qp.memoryadapter;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.GroupCursor;
import com.akiban.qp.operator.IndexScanSelector;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.operator.API.Ordering;
import com.akiban.qp.operator.API.SortOption;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.Store;
import com.akiban.server.types.ValueSource;
import com.akiban.util.tap.InOutTap;

public class MemoryAdapter extends StoreAdapter {

    public MemoryAdapter(Schema schema, 
            Session session,
            ConfigurationService config) {
        super(schema, session, config);
    }

    @Override
    public GroupCursor newGroupCursor(Group group) {
        return new MemoryGroupCursor(this, group);
    }

    @Override
    public <HKEY extends HKey> HKEY newHKey(
            com.akiban.ais.model.HKey hKeyMetadata) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Store getUnderlyingStore() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cursor newIndexCursor(QueryContext context, Index index,
            IndexKeyRange keyRange, Ordering ordering,
            IndexScanSelector scanSelector, boolean usePValues) {
        
        Table table = index.rootMostTable();
        if (table.isUserTable()) {
            return ((UserTable)table).getMemoryTableFactory().getIndexCursor(index, getSession(), keyRange, ordering, scanSelector);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public long rowCount(RowType tableType) {
        long count = 0;
        if (tableType.hasUserTable()) {
            count = tableType.userTable().getMemoryTableFactory().rowCount();
        }
        return count;
    }

    @Override
    public Cursor sort(QueryContext context, Cursor input, RowType rowType,
            Ordering ordering, SortOption sortOption, InOutTap loadTap, boolean usePValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRow(Row oldRow, Row newRow, boolean usePValues) {
        throw new UnsupportedOperationException();
        
    }

    @Override
    public void writeRow(Row newRow, boolean usePValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteRow(Row oldRow, boolean usePValues, boolean cascadeDelete) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterRow(Row oldRow, Row newRow, Index[] indexesToMaintain, boolean hKeyChanged, boolean usePValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sequenceNextValue(TableName sequenceName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sequenceCurrentValue(TableName sequenceName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long hash(ValueSource valueSource, AkCollator collator) {
        return
            collator == null
            ? valueSource.getString().hashCode()
            : collator.hashCode(valueSource.getString());
    }
}
