package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.api.common.IndexId;
import com.akiban.cserver.api.common.TableId;

public final class ScanRequest {

    private final int tableId;
    private final int indexId;
    private final ColumnSet columns;
    private final Predicate predicate;

    public ScanRequest(TableId tableId, IndexId indexId, ColumnSet columns, Predicate... predicates) {
        this.tableId = tableId.getTableId();
        this.indexId = indexId.getIndexId();
        this.columns = new ColumnSet(columns);
        this.predicate = predicates.length == 1 ? predicates[0] : CompositePredicate.ofAnds(predicates);
    }
}
