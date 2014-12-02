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

package com.foundationdb.qp.operator;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.storeadapter.Sorter;
import com.foundationdb.qp.storeadapter.indexcursor.IterationHelper;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.NoSuchSequenceException;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.store.Store;
import com.foundationdb.util.tap.InOutTap;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

public abstract class StoreAdapter
{
    public abstract GroupCursor newGroupCursor(Group group);

    public static final int COMMIT_FREQUENCY_PERIODICALLY = -2;

    public GroupCursor newDumpGroupCursor(Group group, int commitFrequency) {
        return newGroupCursor(group);
    }

    public abstract RowCursor newIndexCursor(QueryContext context,
                                             IndexRowType rowType,
                                             IndexKeyRange keyRange, 
                                             API.Ordering ordering,
                                             IndexScanSelector scanSelector,
                                             boolean openAllSubCursors);
    
    public abstract void updateRow(Row oldRow, Row newRow);

    public void writeRow(Row newRow) {
        writeRow(newRow, null, null);
    }

    public abstract void writeRow(Row newRow, Collection<TableIndex> tableIndexes, Collection<GroupIndex> groupIndexes);
    
    public abstract void deleteRow (Row oldRow, boolean cascadeDelete);

    public abstract Sorter createSorter(QueryContext context,
                                        QueryBindings bindings,
                                        RowCursor input,
                                        RowType rowType,
                                        API.Ordering ordering,
                                        API.SortOption sortOption,
                                        InOutTap loadTap);

    public long getQueryTimeoutMilli() {
        return config.queryTimeoutMilli();
    }

    public long rowCount(Session session, RowType tableType) {
        assert tableType.hasTable() : tableType;
        return tableType.table().rowDef().getTableStatus().getRowCount(session);
    }

    public Sequence getSequence(TableName sequenceName) {
        Sequence sequence = getAIS().getSequence(sequenceName);
        if(sequence == null) {
            throw new NoSuchSequenceException(sequenceName);
        }
        return sequence;
    }

    public abstract long sequenceNextValue(Sequence sequence);

    public abstract long sequenceCurrentValue(Sequence sequence);

    public final Session getSession() {
        return session;
    }

    public abstract IndexRow newIndexRow (IndexRowType indexRowType);
    
    public abstract IndexRow takeIndexRow(IndexRowType indexRowType);

    public abstract void returnIndexRow(IndexRow indexRow);

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
    
    public abstract KeyCreator getKeyCreator();

    protected abstract Store getUnderlyingStore();
    
    public abstract AkibanInformationSchema getAIS();

    protected StoreAdapter(Session session,
            ConfigurationService config)
    {
        this.session = session;
        this.config = config;
    }

    // Class state

    private static final AtomicLong idCounter = new AtomicLong(0);

    // Object state

    private final Session session;
    private final ConfigurationService config;
    private final long id = idCounter.incrementAndGet();
}
