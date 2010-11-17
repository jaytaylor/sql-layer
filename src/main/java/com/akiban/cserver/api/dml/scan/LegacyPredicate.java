package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.IdResolver;

import java.util.EnumSet;
import java.util.Map;

/**
 * <p>An implementation of Predicate that relies on the legacy system of opaque RowData objects for start and end,
 * and a translucent scan flags bitmap.</p>
 *
 * <p>This implementation's NiceRow objects are dumb wrappers for the RowData you provide to the class's
 * constructor. As such, any field access methods will throw UnsupportedOperationException; the toRowData() method
 * will simply return the wrapped RowData. Equality (and hash code) is based on identity.</p>
 *
 * <p>In other words, it's not recommended that you do anything with NiceRows that come from this class except to
 * extract their RowData.</p>
 */
public final class LegacyPredicate implements Predicate {
    private static class LegacyNiceRow extends NiceRow {
        private final RowData rowData;

        private LegacyNiceRow(RowData rowData) {
            this.rowData = rowData;
        }

        @Override
        public Object put(ColumnId index, Object object) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object get(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<ColumnId, Object> getFields() {
            throw new UnsupportedOperationException();
        }

        @Override
        public RowData toRowData(RowDef rowDef, IdResolver ignored) {
            return rowData;
        }

        @Override
        public boolean equals(Object o) {
            return this == o;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }
    }

    private final RowData start;
    private final RowData end;
    private final int scanFlags;

    public LegacyPredicate(RowData start, RowData end, int scanFlags) {
        this.start = start;
        this.end = end;
        this.scanFlags = scanFlags;
    }

    @Override
    public NiceRow getStartRow() {
        return new LegacyNiceRow(start);
    }

    @Override
    public NiceRow getEndRow() {
        return new LegacyNiceRow(end);
    }

    @Override
    public EnumSet<ScanFlag> getScanFlags() {
        return ScanFlag.fromRowDataFormat(scanFlags);
    }
}
