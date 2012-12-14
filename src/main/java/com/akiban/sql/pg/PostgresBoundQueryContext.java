/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.pg;

import com.akiban.qp.operator.CursorBase;
import com.akiban.server.types.AkType;

public class PostgresBoundQueryContext extends PostgresQueryContext 
{
    private static enum State { NORMAL, UNOPENED, SUSPENDED, EXHAUSTED };
    private boolean reportSuspended;
    private State state;
    private PostgresPreparedStatement statement;
    private boolean[] columnBinary;
    private boolean defaultColumnBinary;
    private CursorBase<?> cursor;
    
    public PostgresBoundQueryContext(PostgresServerSession server,
                                     PostgresPreparedStatement statement,
                                     boolean canSuspend, boolean reportSuspended) {
        super(server);
        this.statement = statement;
        this.state = canSuspend ? State.UNOPENED : State.NORMAL;
        this.reportSuspended = reportSuspended;
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
    public <T extends CursorBase> boolean finishCursor(PostgresCursorGenerator<T> generator, T cursor, boolean suspended) {
        if (suspended && (state != State.NORMAL)) {
            this.state = State.SUSPENDED;
            this.cursor = cursor;
            return reportSuspended;
        }
        this.state = State.EXHAUSTED;
        this.cursor = null;
        return super.finishCursor(generator, cursor, suspended);
    }

    protected void close() {
        if (cursor != null) {
            cursor.destroy();
            cursor = null;
            state = State.EXHAUSTED;
        }        
    }

}
