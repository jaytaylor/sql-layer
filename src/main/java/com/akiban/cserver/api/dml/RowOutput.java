package com.akiban.cserver.api.dml;

import com.akiban.cserver.RowData;

public abstract class RowOutput {

    private int count = 0;

    abstract protected void doWrite(RowData data) throws Exception;

    final public void write(RowData data) throws Exception {
        doWrite(data);
        ++count;
    }

    final public int getRowsCount() {
        return count;
    }
}
