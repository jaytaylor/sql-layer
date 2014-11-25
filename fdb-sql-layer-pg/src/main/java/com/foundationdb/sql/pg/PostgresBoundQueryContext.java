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

import com.foundationdb.qp.operator.CursorBase;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.service.monitor.CursorMonitor;

public class PostgresBoundQueryContext extends PostgresQueryContext 
                                       implements CursorMonitor
{
    private QueryBindings bindings;
    private static enum State { NORMAL, UNOPENED, SUSPENDED, EXHAUSTED };
    private boolean reportSuspended;
    private State state;
    private PostgresPreparedStatement statement;
    private boolean[] columnBinary;
    private boolean defaultColumnBinary;
    private CursorBase<?> cursor;
    private String portalName;
    private long creationTime;
    private volatile int nrows;
    
    public PostgresBoundQueryContext(PostgresServerSession server,
                                     PostgresPreparedStatement statement,
                                     String portalName,
                                     boolean canSuspend, boolean reportSuspended) {
        super(server);
        this.statement = statement;
        this.portalName = portalName;
        this.state = canSuspend ? State.UNOPENED : State.NORMAL;
        this.reportSuspended = reportSuspended;
        this.creationTime = System.currentTimeMillis();
    }

    public QueryBindings getBindings() {
        return bindings;
    }

    protected void setBindings(QueryBindings bindings) {
        this.bindings = bindings;
    }

    public PostgresPreparedStatement getStatement() {
        return statement;
    }
    
    protected void setColumnBinary(boolean[] columnBinary, boolean defaultColumnBinary) {
        this.columnBinary = columnBinary;
        this.defaultColumnBinary = defaultColumnBinary;
    }

    @Override
    public boolean isColumnBinary(int i) {
        if ((columnBinary != null) && (i < columnBinary.length))
            return columnBinary[i];
        else
            return defaultColumnBinary;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends CursorBase> T startCursor(PostgresCursorGenerator<T> generator, QueryBindings bindings) {
        switch (state) {
        case NORMAL:
        case UNOPENED:
        default:
            return super.startCursor(generator, bindings);
        case SUSPENDED:
            return (T)cursor;
        case EXHAUSTED:
            return null;
        }
    }

    @Override
    public <T extends CursorBase> boolean finishCursor(PostgresCursorGenerator<T> generator, T cursor, int nrows, boolean suspended) {
        this.nrows += nrows;
        if (suspended && (state != State.NORMAL)) {
            this.state = State.SUSPENDED;
            this.cursor = cursor;
            return reportSuspended;
        }
        this.state = State.EXHAUSTED;
        this.cursor = null;
        return super.finishCursor(generator, cursor, nrows, suspended);
    }

    protected void close() {
        if (cursor != null) {
            cursor.close();
            cursor = null;
            state = State.EXHAUSTED;
        }        
    }

    /* CursorMonitor */

    @Override
    public int getSessionId() {
        return getServer().getSessionMonitor().getSessionId();
    }

    @Override
    public String getName() {
        return portalName;
    }

    @Override
    public String getSQL() {
        return statement.getSQL();
    }

    @Override
    public String getPreparedStatementName() {
        return statement.getName();
    }

    @Override
    public long getCreationTimeMillis() {
        return creationTime;
    }

    @Override
    public int getRowCount() {
        return nrows;
    }

}
