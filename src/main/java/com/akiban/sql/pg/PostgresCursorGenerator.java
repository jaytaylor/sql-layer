
package com.akiban.sql.pg;

import com.akiban.qp.operator.CursorBase;

public interface PostgresCursorGenerator<T extends CursorBase> {
    public boolean canSuspend(PostgresServerSession server);
    public T openCursor(PostgresQueryContext context);
    public void closeCursor(T cursor);
}
