
package com.akiban.qp.rowtype;

import com.akiban.qp.row.Row;
import com.akiban.server.error.InvalidOperationException;

public interface ConstraintChecker
{
    /** Check constraints on row.
     * @throws com.akiban.server.error.InvalidOperationException thrown if a constraint on the row is violated.
     */
    public void checkConstraints(Row row, boolean usePValues) throws InvalidOperationException;
}
