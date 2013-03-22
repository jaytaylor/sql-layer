
package com.akiban.qp.operator;

import com.akiban.qp.row.Row;

public interface UpdateFunction extends SelectionFunction {
    /**
     * Updates the given row by returning another row with the required modifications.
     * @param original the original row, which will remain untouched
     * @param context the query context for evaluation
     * @return a row of the same type as the original, but different fields
     * @throws IllegalArgumentException if the row could not be updated
     * (ie, if {@linkplain #rowIsSelected(Row)} returned {@code false})
     */
    Row evaluate(Row original, QueryContext context);
    boolean usePValues();
}
