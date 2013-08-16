/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.api.dml.scan;

import java.util.EnumSet;
import java.util.Set;

import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.util.ArgumentValidation;

public final class SimplePredicate implements Predicate {
    public enum Comparison {
        EQ, LT, LTE, GT, GTE
    }

    private NewRow startRow = null;
    private NewRow endRow = null;
    private final int tableId;
    private final Comparison comparison;
    private final Set<ScanFlag> scanFlags = EnumSet.noneOf(ScanFlag.class);

    public SimplePredicate(int tableId, Comparison comparison) {
        ArgumentValidation.notNull("comparison operator", comparison);
        this.comparison = comparison;
        this.tableId = tableId;
    }

    public void addColumn(int column, Object value) {
        ArgumentValidation.notNull("column ID", column);
        ArgumentValidation.notNull("value", value); // TODO verify this is needed

        switch (comparison) {
            case EQ:
                if (startRow == null) {
                    assert endRow == null : endRow;
                    startRow = new NiceRow(tableId, (RowDef)null);
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
                    endRow = new NiceRow(tableId, (RowDef)null);
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
                    startRow = new NiceRow(tableId, (RowDef)null);
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

    private void putToRow(NewRow which, int index, Object value) {
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
