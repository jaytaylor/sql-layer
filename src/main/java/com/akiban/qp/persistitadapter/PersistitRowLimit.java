
package com.akiban.qp.persistitadapter;

import com.akiban.qp.operator.Limit;
import com.akiban.qp.row.RowBase;
import com.akiban.server.api.dml.scan.ScanLimit;

public class PersistitRowLimit implements Limit
{
    // Object interface

    @Override
    public String toString()
    {
        return limit.toString();
    }


    // Limit interface

    @Override
    public boolean limitReached(RowBase row)
    {
        PersistitGroupRow persistitRow = (PersistitGroupRow) row;
        return limit.limitReached(persistitRow.rowData());
    }

    // PersistitRowLimit interface

    public PersistitRowLimit(ScanLimit limit)
    {
        this.limit = limit;
    }

    // Object state

    private final ScanLimit limit;
}
