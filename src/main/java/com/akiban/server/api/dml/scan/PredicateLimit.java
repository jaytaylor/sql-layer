
package com.akiban.server.api.dml.scan;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.util.ArgumentValidation;

public final class PredicateLimit implements ScanLimit {
    private final int rowDefId;
    private final int limit;
    private int count = 0;

    public PredicateLimit(int rowDefId, int limit) {
        ArgumentValidation.isGTE("limit", limit, 0);
        this.limit = limit;
        this.rowDefId = rowDefId;
    }

    @Override
    public String toString()
    {
        return Integer.toString(limit);
    }

    @Override
    public boolean limitReached(RowData candidateRow) {
        if (candidateRow != null && candidateRow.getRowDefId() == rowDefId) {
            ++count;
        }
        return limit == 0 || count > limit;
    }

    public int getLimit() {
        return limit;
    }
}
