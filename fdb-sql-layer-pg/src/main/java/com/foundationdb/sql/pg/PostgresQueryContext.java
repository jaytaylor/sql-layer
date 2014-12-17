/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.pg;

import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.sql.server.ServerQueryContext;

import com.foundationdb.qp.operator.CursorBase;
import com.foundationdb.qp.operator.QueryBindings;

public class PostgresQueryContext extends ServerQueryContext<PostgresServerSession>
{
    public PostgresQueryContext(PostgresServerSession server) {
        super(server);
    }

    public PostgresQueryContext(PostgresServerSession server, Schema schema) {
        super(server);
    }

    public boolean isColumnBinary(int i) {
        return false;
    }

    /**
     * Returns an open cursor.
     * finishCursor must be called with the returned cursor in a finally block.
     * Do not call close() on the returned cursor, call finishCursor.
     * If finish cursor was previously called with suspended=true, that cursor may be returned here.
     */
    public <T extends CursorBase> T startCursor(PostgresCursorGenerator<T> generator, QueryBindings bindings) {
        return generator.openCursor(this, bindings);
    }

    /**
     *
     * @param cursor a cursor as returned by startCursor
     * @param nrows the number of rows processed since the last call to startCursor
     * @param suspended if true the cursor is just suspended, call startCursor again to resume processing
     */
    public <T extends CursorBase> boolean finishCursor(PostgresCursorGenerator<T> generator, T cursor, int nrows, boolean suspended) {
        generator.closeCursor(cursor);
        return false;
    }

}
