package com.akiban.cserver.api.dml;

import com.akiban.cserver.RowData;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.IndexId;
import com.akiban.cserver.api.common.TableId;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

public final class ScanRequest {
    public enum Flags {
        ORDER_DESC,
        START_RANGE_EXCLUSIVE,
        END_RANGE_EXCLUSIVE,
        SINGLE_ROW,
        USE_PREFIXES,
        START_AT_EDGE,
        END_AT_EDGE,
        DEEP
    }

    public final static class ColumnSet {
        private final Set<ColumnId> columns;

        public ColumnSet() {
            this.columns = new HashSet<ColumnId>();
        }

        ColumnSet(ColumnSet copy) {
            this.columns = new HashSet<ColumnId>(copy.columns);
        }

        public void addColumn(ColumnId column) {
            columns.add(column);
        }

        public void removeColumn(ColumnId column) {
            columns.remove(column);
        }
    }

    private final int tableId;
    private final int indexId;
    private final EnumSet<Flags> scanFlags;
    private final ColumnSet columns;
    private final RowData start;
    private final RowData end;

    public ScanRequest(TableId tableId, IndexId indexId, EnumSet<Flags> scanFlags, ColumnSet columns,
                       RowData start, RowData end) {
        this.tableId = tableId.getTableId();
        this.indexId = indexId.getIndexId();
        this.scanFlags = EnumSet.copyOf(scanFlags);
        this.columns = new ColumnSet(columns);
        this.start = start;
        this.end = end;
    }
}
