package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.api.common.*;
import com.akiban.cserver.api.dml.NoSuchTableException;

import java.util.Set;

public final class ScanRequest {

    private final TableId tableId;
    private final IndexId indexId;
    private final Set<ColumnId> columns;
    private final Predicate predicate;

    public ScanRequest(TableId tableId, IndexId indexId, Set<ColumnId> columns, Predicate predicate) {
        this.tableId = tableId;
        this.indexId = indexId;
        this.columns = columns;
        this.predicate = predicate;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public byte[] getColumnSetBytes(IdResolver resolver) {
        return ColumnSet.packToLegacy(columns, resolver);
    }

    public int getTableIdInt(IdResolverImpl resolver) throws NoSuchTableException {
        return tableId.getTableId(resolver);
    }

    public int getIndexIdInt(IdResolver resolver) {
        return indexId.getIndexId(resolver);
    }
}
