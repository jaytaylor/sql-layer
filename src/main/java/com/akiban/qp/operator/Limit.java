
package com.akiban.qp.operator;

import com.akiban.qp.row.RowBase;

public interface Limit
{
    boolean limitReached(RowBase row);
}
