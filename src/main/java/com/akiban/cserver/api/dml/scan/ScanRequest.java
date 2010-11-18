package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.api.common.*;
import com.akiban.cserver.api.dml.NoSuchTableException;

import java.util.Set;

public final class ScanRequest extends ScanRange {

    private final IndexId indexId;

    public ScanRequest(TableId tableId, IndexId indexId, Set<ColumnId> columns, Predicate predicate) {
        super(tableId, columns, predicate);
        this.indexId = indexId;
    }

    public int getIndexIdInt(IdResolver resolver) {
        return indexId.getIndexId(resolver);
    }
}
