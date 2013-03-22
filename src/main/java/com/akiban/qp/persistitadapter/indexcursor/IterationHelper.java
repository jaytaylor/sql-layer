
package com.akiban.qp.persistitadapter.indexcursor;

import com.akiban.qp.row.Row;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

public interface IterationHelper
{
    Row row() throws PersistitException;
    void openIteration();
    void closeIteration();
    Exchange exchange();
}
