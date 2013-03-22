
package com.akiban.sql.pg;

import com.akiban.sql.server.ServerQueryContext;

import com.akiban.qp.operator.CursorBase;

public class PostgresQueryContext extends ServerQueryContext<PostgresServerSession>
{
    public PostgresQueryContext(PostgresServerSession server) {
        super(server);
    }

    public boolean isColumnBinary(int i) {
        return false;
    }

    public <T extends CursorBase> T startCursor(PostgresCursorGenerator<T> generator) {
        return generator.openCursor(this);
    }

    public <T extends CursorBase> boolean finishCursor(PostgresCursorGenerator<T> generator, T cursor, int nrows, boolean suspended) {
        generator.closeCursor(cursor);
        return false;
    }

}
