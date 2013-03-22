
package com.akiban.qp.rowtype;

import com.akiban.qp.row.Row;
import com.akiban.server.error.InvalidOperationException;

public class NoopConstraintChecker implements ConstraintChecker
{
    @Override
    public void checkConstraints(Row row, boolean usePValues) throws InvalidOperationException
    {
    }
}
