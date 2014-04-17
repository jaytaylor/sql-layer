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

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.IndexScanSelector;
import com.foundationdb.qp.operator.API.Ordering;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.statistics.IndexStatistics;

public abstract class BasicFactoryBase implements MemoryTableFactory {
    private final TableName sourceTable;
    
    public BasicFactoryBase(TableName sourceTable) {
        this.sourceTable = sourceTable;
    }

    @Override
    public TableName getName() {
        return sourceTable;
    }

    @Override
    public Cursor getIndexCursor(Index index, Session session,
            IndexKeyRange keyRange, Ordering ordering,
            IndexScanSelector scanSelector) {
        throw new UnsupportedOperationException();
    }

    public RowType getRowType(MemoryAdapter adapter) {
        return adapter.schema().tableRowType(adapter.schema().ais().getTable(sourceTable));
    }
    
    public static String boolResult(boolean bool) {
        return bool ? "YES" : "NO";
    }
}
