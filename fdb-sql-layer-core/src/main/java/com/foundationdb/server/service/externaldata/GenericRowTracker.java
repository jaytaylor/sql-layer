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

package com.foundationdb.server.service.externaldata;

import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;

import java.util.ArrayList;
import java.util.List;

public abstract class GenericRowTracker implements RowTracker {
    private final List<RowType> openTypes = new ArrayList<>(3);
    private RowType curRowType;
    private int curDepth;

    protected void setDepth(int depth) {
        curDepth = depth;
    }

    @Override
    public void reset() {
        curRowType = null;
        curDepth = 0;
        openTypes.clear();
    }

    @Override
    public int getMinDepth() {
        return 0;
    }

    @Override
    public int getMaxDepth() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void beginRow(Row row) {
        curRowType = row.rowType();
    }

    @Override
    public int getRowDepth() {
        return curDepth;
    }

    @Override
    public boolean isSameRowType() {
        return (getRowDepth() < openTypes.size()) &&
               (curRowType == openTypes.get(getRowDepth()));
    }

    @Override
    public void pushRowType() {
        openTypes.add(curRowType);
    }

    @Override
    public void popRowType() {
    }
}
