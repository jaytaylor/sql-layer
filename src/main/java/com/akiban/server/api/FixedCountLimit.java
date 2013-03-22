
package com.akiban.server.api;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.util.ArgumentValidation;

public class FixedCountLimit implements ScanLimit {
    private final int limit;
    private int count = 0;

    public FixedCountLimit(int limit) {
        ArgumentValidation.isGTE("limit", limit, 0);
        this.limit = limit;
    }

    @Override
    public boolean limitReached(RowData previousRow) {
        return limit >= 0 && count++ >= limit;
    }

    public int getLimit() {
        return limit;
    }
}
