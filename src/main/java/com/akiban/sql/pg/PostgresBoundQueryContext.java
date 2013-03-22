
package com.akiban.sql.pg;

import com.akiban.qp.operator.CursorBase;
import com.akiban.server.types.AkType;
import com.akiban.server.service.monitor.CursorMonitor;

public class PostgresBoundQueryContext extends PostgresQueryContext 
                                       implements CursorMonitor
{
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
    public <T extends CursorBase> T startCursor(PostgresCursorGenerator<T> generator) {
        switch (state) {
        case NORMAL:
        case UNOPENED:
        default:
            return super.startCursor(generator);
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
            cursor.destroy();
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
