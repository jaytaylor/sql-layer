/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.api.dml.scan;

import java.util.EnumSet;
import java.util.Set;

import com.akiban.server.rowdata.RowDef;
import com.akiban.util.ArgumentValidation;

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
