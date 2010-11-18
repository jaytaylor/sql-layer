package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.NoSuchTableException;

import java.util.Set;

public class ScanRange {
    protected final TableId tableId;
    protected final Set<ColumnId> columns;
    protected final Predicate predicate;

    public ScanRange(TableId tableId, Set<ColumnId> columns, Predicate predicate) {
        this.tableId = tableId;
        this.columns = columns;
        this.predicate = predicate;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public byte[] getColumnSetBytes(IdResolver resolver) {
        return ColumnSet.packToLegacy(columns, resolver);
    }

    public int getTableIdInt(IdResolver resolver) throws NoSuchTableException {
        return tableId.getTableId(resolver);
    }
}
