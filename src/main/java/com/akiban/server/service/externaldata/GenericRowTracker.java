
package com.akiban.server.service.externaldata;

import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;

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
}
