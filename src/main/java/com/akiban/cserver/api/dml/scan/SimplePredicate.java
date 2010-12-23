package com.akiban.cserver.api.dml.scan;

import java.util.EnumSet;
import java.util.Set;

import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.TableId;
import com.akiban.util.ArgumentValidation;

public final class SimplePredicate implements Predicate {
    public enum Comparison {
        EQ, LT, LTE, GT, GTE
    }

    private NewRow startRow = null;
    private NewRow endRow = null;
    private final TableId tableId;
    private final Comparison comparison;
    private final Set<ScanFlag> scanFlags = EnumSet.noneOf(ScanFlag.class);

    public SimplePredicate(TableId tableId, Comparison comparison) {
        ArgumentValidation.notNull("comparison operator", comparison);
        this.comparison = comparison;
        this.tableId = tableId;
    }

    public void addColumn(ColumnId column, Object value) {
        ArgumentValidation.notNull("column ID", column);
        ArgumentValidation.notNull("value", value); // TODO verify this is needed

        switch (comparison) {
            case EQ:
                if (startRow == null) {
                    assert endRow == null : endRow;
                    startRow = new NiceRow(tableId);
                    endRow = startRow;
                    scanFlags.add(ScanFlag.START_RANGE_EXCLUSIVE);
                    scanFlags.add(ScanFlag.END_RANGE_EXCLUSIVE);
                }
                assert startRow == endRow : String.format("%s != %s" , startRow, endRow);
                putToRow(startRow, column, value);
                break;
            case LT:
            case LTE:
                if (endRow == null) {
                    endRow = new NiceRow(tableId);
                }
                putToRow(endRow, column, value);
                scanFlags.add(ScanFlag.START_AT_BEGINNING);
                if (comparison.equals(Comparison.LT)) {
                    scanFlags.add(ScanFlag.END_RANGE_EXCLUSIVE);
                }
                break;
            case GT:
            case GTE:
                if (startRow == null) {
                    startRow = new NiceRow(tableId);
                }
                putToRow(startRow, column, value);
                scanFlags.add(ScanFlag.END_AT_END);
                if (comparison.equals(Comparison.GT)) {
                    scanFlags.add(ScanFlag.START_RANGE_EXCLUSIVE);
                }
                break;
            default:
                throw new IllegalArgumentException("Unrecognized comparison: " + comparison.name());
        }
    }

    private void putToRow(NewRow which, ColumnId index, Object value) {
        Object old = which.put(index, value);
        if (old != null && (!old.equals(value))) {
            which.put(index, old);
            throw new IllegalStateException(String.format("conflict at index %s: %s != %s", index, value, old));
        }
    }

    @Override
    public NewRow getStartRow() {
        return startRow;
    }

    @Override
    public NewRow getEndRow() {
        return endRow;
    }

    @Override
    public EnumSet<ScanFlag> getScanFlags() {
        return EnumSet.copyOf(scanFlags);
    }
}
