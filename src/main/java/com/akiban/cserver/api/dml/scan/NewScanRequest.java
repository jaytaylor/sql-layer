package com.akiban.cserver.api.dml.scan;

import java.util.Set;

import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.cserver.api.common.IndexId;
import com.akiban.cserver.api.common.TableId;

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
