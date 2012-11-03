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

import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.Store;

/**
 * Convenience class for building NewRows. Primarily useful for debugging.
 */
public final class NewRowBuilder {
    private final NewRow row;
    private int nextCol = 0;

    public static NewRowBuilder forTable(int tableId, RowDef rowDef) {
        return new NewRowBuilder(new NiceRow(tableId, rowDef));
    }

    public static NewRowBuilder copyOf(NewRow row) {
        NewRowBuilder builder = forTable(row.getTableId(), row.getRowDef());
        builder.row().getFields().putAll( row.getFields() );
        return builder;
    }

    public NewRowBuilder(NewRow row) {
        this.row = row;
    }

    /**
     * Puts the given column-to-object pairing in, and returns this builder for chaining.
     * @param column the column
     * @param object the object
     * @return this instance
     */
    public NewRowBuilder put(int column, Object object) {
        row.put(column, object);
        nextCol = column + 1;
        return this;
    }

    /**
     * <p>Puts a value into the next column. The "next" column is defined as the column whose position is one greater
     * than the last column to be put. If no column has been put yet, this method puts to the 0th column.</p>
     *
     * <p>Note that if you invoke <tt>put(1, obj1)</tt> then <tt>put(0, obj2)</tt>, a call to <tt>put(obj3)</tt> will
     * override the first put's value, since the last put was at 0, so the next one will be at 1.</p>
     * @param object the object to put
     * @return this instance
     */
    public NewRowBuilder put(Object object) {
        return put(nextCol, object);
    }

    /**
     * Appends all of the given row's values to this builder's row. This method uses only the values Collection of the
     * given row; the ColumnIds they map to are ignored. This is helpful in constructing a group table's row
     * from the rows of its constituent user table's rows.
     * @param row the row to append to this builder's row
     * @return this builder
     */
    public NewRowBuilder append(NewRow row) {
        for (Object obj : row.getFields().values()) {
            put(obj);
        }
        return this;
    }

    /**
     * Tries to convert this row to a RowData; mostly good as a debug line. This method converts the row to a RowData,
     * converts that RowData back, and throws a runtime exception if the two rows are not equal.
     * @param dml the DMLFunctions that is responsible for the conversion
     * @return this builder
     */
    public NewRowBuilder check(Session session, DMLFunctions dml) {
        final RowData rowData = dml.convertNewRow(row);
        final NewRow back = dml.convertRowData(session, rowData);
        if (!row.equals(back)) {
            throw new RuntimeException(String.format("%s != %s", row, back));
        }
        return this;
    }

    /**
     * Returns the Row that has been built.
     * @return the row
     */
    public NewRow row() {
        return row;
    }
}
