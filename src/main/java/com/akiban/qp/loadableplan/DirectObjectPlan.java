
package com.akiban.qp.loadableplan;

import com.akiban.qp.operator.QueryContext;

/** A plan that uses a {@link DirectObjectCursor}. */
public abstract class DirectObjectPlan
{
    public abstract DirectObjectCursor cursor(QueryContext context);

    public enum OutputMode { TABLE, COPY_WITH_NEWLINE, COPY };

    /** Return <code>COPY</code> to stream a single column with text formatting. */
    public OutputMode getOutputMode() {
        return OutputMode.TABLE;
    }
}
