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
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.persistitadapter.RowDataCreator;
import com.akiban.qp.persistitadapter.Sorter;
import com.akiban.qp.persistitadapter.indexcursor.IterationHelper;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRow;
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.KeyCreator;
import com.akiban.server.store.Store;
import com.akiban.server.types.ValueSource;
import com.akiban.util.tap.InOutTap;

import java.util.concurrent.atomic.AtomicLong;

public abstract class StoreAdapter implements KeyCreator
{
    public abstract GroupCursor newGroupCursor(Group group);

    public abstract RowCursor newIndexCursor(QueryContext context,
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

    public abstract void writeRow (Row newRow, Index[] indexes, boolean usePValues);
    
    public abstract void deleteRow (Row oldRow, boolean usePValues, boolean cascadeDelete);

    public abstract Sorter createSorter(QueryContext context,
                                        QueryBindings bindings,
                                        Cursor input,
                                        RowType rowType,
                                        API.Ordering ordering,
                                        API.SortOption sortOption,
                                        InOutTap loadTap);

    public long getQueryTimeoutMilli() {
        return config.queryTimeoutMilli();
    }

    public abstract long rowCount(Session session, RowType tableType);
    
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

    public static NewRow newRow(RowDef rowDef)
    {
        NiceRow row = new NiceRow(rowDef.getRowDefId(), rowDef);
        UserTable table = rowDef.userTable();
        PrimaryKey primaryKey = table.getPrimaryKeyIncludingInternal();
        if(primaryKey != null && table.getPrimaryKey() == null) {
            // Akiban-generated PK. Initialize its value to a dummy value, which will be replaced later. The
            // important thing is that the value be non-null.
            row.put(table.getColumnsIncludingInternal().size() - 1, -1L);
        }
        return row;
    }

    public <S> RowData rowData(RowDef rowDef, RowBase row, RowDataCreator<S> creator) {
        // Generic conversion, subclasses should override to check for known group rows
        NewRow niceRow = newRow(rowDef);
        for(int i = 0; i < row.rowType().nFields(); ++i) {
            S source = creator.eval(row, i);
            creator.put(source, niceRow, rowDef.getFieldDef(i), i);
        }
        return niceRow.toRowData();
    }

    public abstract PersistitIndexRow takeIndexRow(IndexRowType indexRowType);

    public abstract void returnIndexRow(PersistitIndexRow indexRow);

    public abstract IterationHelper createIterationHelper(IndexRowType indexRowType);

    public long id() {
        return id;
    }

    public enum AdapterType {
        STORE_ADAPTER,
        MEMORY_ADAPTER
    }
    
    public final ConfigurationService getConfig() {
        return config;
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
    private static final AtomicLong idCounter = new AtomicLong(0);

    // Object state

    protected final Schema schema;
    private final Session session;
    private final ConfigurationService config;
    private final long id = idCounter.incrementAndGet();
}
