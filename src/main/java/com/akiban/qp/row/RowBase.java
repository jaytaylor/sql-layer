
package com.akiban.qp.row;

import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.rowtype.RowType;

public interface RowBase extends BoundExpressions
{
    RowType rowType();
    HKey hKey();
    HKey ancestorHKey(UserTable table);
    boolean ancestorOf(RowBase that);
    boolean containsRealRowOf(UserTable userTable);
    Row subRow(RowType subRowType);

    /**
     * Compares two rows and indicates if and where they differ.
     * @param row The row to be compared to this row.
     * @param leftStartIndex First field to compare in this row.
     * @param rightStartIndex First field to compare in the other row.
     * @param fieldCount Number of fields to compare.
     * @return 0 if all fields are equal. A negative value indicates that this row had the first field
     * that was not equal to the corresponding field in the other row. A positive value indicates that the
     * other row had the first field that was not equal to the corresponding field in this row. In both non-zero
     * cases, the absolute value of the return value is the position of the field that differed, starting the numbering
     * at 1. E.g. a return value of -2 means that the first fields of the rows match, and that in the second field,
     * this row had the smaller value.
     */
    int compareTo(RowBase row, int leftStartIndex, int rightStartIndex, int fieldCount);
}
