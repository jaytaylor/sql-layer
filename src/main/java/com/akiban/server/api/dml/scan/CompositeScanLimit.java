
package com.akiban.server.api.dml.scan;

import com.akiban.server.rowdata.RowData;

public final class CompositeScanLimit implements ScanLimit {
    private final ScanLimit[] limits;

    public CompositeScanLimit(ScanLimit... limits) {
        this.limits = new ScanLimit[limits.length];
        System.arraycopy(limits, 0, this.limits, 0, limits.length);
    }

    @Override
    public boolean limitReached(RowData previousRow) {
        for (ScanLimit limit : limits) {
            if (limit.limitReached(previousRow)) {
                return true;
            }
        }
        return false;
    }
}
