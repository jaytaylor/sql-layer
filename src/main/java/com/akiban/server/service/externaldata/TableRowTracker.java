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

package com.akiban.server.service.externaldata;

import com.akiban.ais.model.NopVisitor;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;

public class TableRowTracker implements RowTracker {
    private final int minDepth;
    private final int maxDepth;
    // This is not sufficient if orphans are possible (when
    // ancestor keys are repeated in descendants). In that case, we
    // have to save rows and check that they are ancestors of new
    // rows, discarding any that are not.
    private final RowType[] openTypes;

    private RowType curRowType;
    private UserTable curTable;

    public TableRowTracker(UserTable table, int addlDepth) {
        minDepth = table.getDepth();
        final int max[] = { minDepth };
        if (addlDepth < 0) {
            table.traverseTableAndDescendants(new NopVisitor() {
                @Override
                public void visitUserTable(UserTable userTable) {
                    max[0] = Math.max(max[0], userTable.getDepth());
                }
            });
        }
        else {
            max[0] += addlDepth;
        }
        maxDepth = max[0];
        openTypes = new RowType[maxDepth+1];
    }

    @Override
    public void reset() {
        curRowType = null;
        curTable = null;
    }

    @Override
    public int getMinDepth() {
        return minDepth;
    }

    @Override
    public int getMaxDepth() {
        return maxDepth;
    }

    @Override
    public void beginRow(Row row) {
        assert row.rowType().hasUserTable() : "Invalid row type for TableRowTracker";
        curRowType = row.rowType();
        curTable = curRowType.userTable();
    }

    @Override
    public int getRowDepth() {
        return curTable.getDepth();
    }

    @Override
    public String getRowName() {
        return curTable.getNameForOutput();
    }

    @Override
    public boolean isSameRowType() {
        return curRowType == openTypes[getRowDepth()];
    }

    @Override
    public void pushRowType() {
        openTypes[getRowDepth()] = curRowType;
    }

    @Override
    public void popRowType() {
    }
}
