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

package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;
import com.akiban.sql.optimizer.plan.MultiIndexEnumerator.MultiIndexPair;

import java.util.List;

public final class MultiIndexIntersectScan extends IndexScan {
    
    private MultiIndexPair<ColumnExpression> index;
    
    public MultiIndexIntersectScan(TableSource table, MultiIndexPair<ColumnExpression> index) {
        super(table);
        this.index = index;
    }

    public MultiIndexIntersectScan(TableSource rootMostTable, TableSource leafMostTable,
                                   MultiIndexPair<ColumnExpression> index)
    {
        super(rootMostTable, rootMostTable, leafMostTable, leafMostTable);
        Index idx = index.getOutputIndex().getIndex();
        checkTablesMatch(idx.rootMostTable(), rootMostTable);
        checkTablesMatch(idx.leafMostTable(), leafMostTable);
        this.index = index;
    }

    private static void checkTablesMatch(Table aisTable, TableSource tableSource) {
        assert aisTable == tableSource.getTable().getTable() : aisTable + " != " + tableSource;
    }

    @Override
    public List<IndexColumn> getKeyColumns() {
        return index.getOutputIndex().getIndex().getKeyColumns();
    }

    @Override
    protected String summarizeIndex() {
        return String.valueOf(index);
    }

    @Override
    protected boolean isAscendingAt(int index) {
        throw new UnsupportedOperationException(); // TODO do I use the output index?
    }
}
