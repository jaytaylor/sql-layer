/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.rowtype.IndexKeyType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.PersistitStore;

import java.util.ArrayList;
import java.util.List;

public class OneTableRowCollector extends OperatorBasedRowCollector
{
    OneTableRowCollector(Session session,
                         PersistitStore store,
                         RowDef rowDef,
                         int indexId,
                         int scanFlags,
                         RowData start,
                         RowData end)
    {
        super(store, session);
        // rootmostQueryTable
        queryRootTable = rowDef.userTable();
        // predicateIndex and predicateType
        predicateIndex = null;
        for (Index userTableIndex : queryRootTable.getIndexesIncludingInternal()) {
            if (userTableIndex.getIndexId() == indexId) {
                predicateIndex = userTableIndex;
            }
        }
        assert predicateIndex != null : String.format("table %s, index %s", queryRootTable, indexId);
        predicateType = schema.userTableRowType((UserTable) predicateIndex.getTable());
        // Index bounds
        assert start == null || start.getRowDefId() == queryRootTable.getTableId();
        assert end == null || end.getRowDefId() == queryRootTable.getTableId();
        IndexKeyType indexKeyType = predicateType.indexRowType(predicateIndex).keyType();
        IndexBound lo =
            start == null
            ? null
            : new IndexBound(indexKeyType, PersistitGroupRow.newPersistitGroupRow(adapter, start));
        IndexBound hi =
            end == null
            ? null
            : new IndexBound(indexKeyType, PersistitGroupRow.newPersistitGroupRow(adapter, end));
        indexKeyRange = new IndexKeyRange(lo,
                                          (scanFlags & (SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_START_EXCLUSIVE)) == 0,
                                          hi,
                                          (scanFlags & (SCAN_FLAGS_END_AT_EDGE | SCAN_FLAGS_END_EXCLUSIVE)) == 0);
    }
}
