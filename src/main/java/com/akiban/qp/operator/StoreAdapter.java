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

package com.akiban.qp.operator;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.HKey;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.persistitadapter.Sorter;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.Store;
import com.akiban.server.types.ValueSource;
import com.akiban.util.tap.InOutTap;

public abstract class StoreAdapter
{
    public abstract GroupCursor newGroupCursor(Group group);

    public abstract Cursor newIndexCursor(QueryContext context,
                                          Index index,
                                          IndexKeyRange keyRange,
                                          API.Ordering ordering,
                                          IndexScanSelector scanSelector,
                                          boolean usePValues);

    public abstract <HKEY extends com.akiban.qp.row.HKey> HKEY newHKey(HKey hKeyMetadata);

    public final Schema schema()
    {
        return schema;
    }

    public abstract void updateRow(Row oldRow, Row newRow, boolean usePValues);
    
    public abstract void writeRow (Row newRow, boolean usePValues);
    
    public abstract void deleteRow (Row oldRow, boolean usePValues, boolean cascadeDelete);

    public abstract void alterRow(Row oldRow, Row newRow, Index[] indexesToMaintain, boolean hKeyChanged, boolean usePValues);

    public abstract Sorter createSorter(QueryContext context,
                                        Cursor input,
                                        RowType rowType,
                                        API.Ordering ordering,
                                        API.SortOption sortOption,
                                        InOutTap loadTap);

    public long getQueryTimeoutMilli() {
        return config.queryTimeoutMilli();
    }

    public long rowCount(Session session, RowType tableType) {
        assert tableType.hasUserTable() : tableType;
        return tableType.userTable().rowDef().getTableStatus().getRowCount(session);
    }
    
    public abstract long sequenceNextValue(TableName sequenceName);

    public abstract long sequenceCurrentValue(TableName sequenceName);

    public abstract long hash(ValueSource valueSource, AkCollator collator);

    // Persistit Transaction step related. Way to generalize?
    public int enterUpdateStep() {
        throw new UnsupportedOperationException(getClass().getSimpleName());
    }

    public int enterUpdateStep(boolean evenIfZero) {
        throw new UnsupportedOperationException(getClass().getSimpleName());
    }

    public void leaveUpdateStep(int step) {
        throw new UnsupportedOperationException(getClass().getSimpleName());
    }

    public void withStepChanging(boolean withStepChanging) {
        throw new UnsupportedOperationException(getClass().getSimpleName());
    }

    public final Session getSession() {
        return session;
    }

    public boolean isBulkloading() {
        return getUnderlyingStore().isBulkloading();
    }

    public enum AdapterType {
        STORE_ADAPTER,
        MEMORY_ADAPTER
    }
    
    public final ConfigurationService getConfig() {
        return config;
    }

    public void setBulkload(Session session, boolean bulkload) {
        if (bulkload == isBulkloading())
            return;
        if (bulkload)
            getUnderlyingStore().startBulkLoad(session);
        else
            getUnderlyingStore().finishBulkLoad(session);
    }

    protected abstract Store getUnderlyingStore();

    protected StoreAdapter(Schema schema,
            Session session,
            ConfigurationService config)
    {
        this.schema = schema;
        this.session = session;
        this.config = config;
    }

    // Class state

    public static final Session.Key<StoreAdapter> STORE_ADAPTER_KEY = Session.Key.named("STORE_ADAPTER");

    // Object state

    protected final Schema schema;
    private final Session session;
    private final ConfigurationService config;

}
