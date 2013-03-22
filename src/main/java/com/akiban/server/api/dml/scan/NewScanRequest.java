
package com.akiban.server.api.dml.scan;

import java.util.Set;

public final class NewScanRequest extends NewScanRange {

    private final int indexId;

    public NewScanRequest(int tableId, int indexId, Set<Integer> columns, Predicate predicate) {
        super(tableId, columns, predicate);
        this.indexId = indexId;
    }

    public int getIndexIdInt() {
        return indexId;
    }
}
