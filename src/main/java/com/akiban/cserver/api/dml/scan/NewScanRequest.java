package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.api.common.*;

import java.util.Set;

public final class NewScanRequest extends NewScanRange {

    private final IndexId indexId;

    public NewScanRequest(TableId tableId, IndexId indexId, Set<ColumnId> columns, Predicate predicate) {
        super(tableId, columns, predicate);
        this.indexId = indexId;
    }

    public int getIndexIdInt(IdResolver resolver) {
        return indexId.getIndexId(resolver);
    }
}
