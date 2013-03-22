
package com.akiban.server.service.externaldata;

import com.akiban.qp.row.Row;

public interface RowTracker {
    void reset();
    int getMinDepth();
    int getMaxDepth();

    void beginRow(Row row);
    int getRowDepth();
    String getRowName();

    void pushRowType();
    boolean isSameRowType();
}
