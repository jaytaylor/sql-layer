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

import com.foundationdb.ais.model.Column;
import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerCallInvocation;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.loadableplan.LoadableDirectObjectPlan;
import com.foundationdb.qp.loadableplan.DirectObjectPlan;
import com.foundationdb.qp.loadableplan.DirectObjectCursor;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;

import java.util.List;
import java.io.IOException;

public class PostgresLoadableDirectObjectPlan extends PostgresDMLStatement
                                       implements PostgresCursorGenerator<DirectObjectCursor>
{
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresLoadableDirectObjectPlan: execute shared");

    private ServerCallInvocation invocation;
    private DirectObjectPlan plan;
    private DirectObjectPlan.OutputMode outputMode;

    protected PostgresLoadableDirectObjectPlan(LoadableDirectObjectPlan loadablePlan,
                                               ServerCallInvocation invocation,
                                               List<String> columnNames, List<PostgresType> columnTypes, List<Column> aisColumns,
                                               PostgresType[] parameterTypes)
    {
        super.init(null, columnNames, columnTypes, aisColumns, parameterTypes);
        this.invocation = invocation;
        plan = loadablePlan.plan();
        outputMode = plan.getOutputMode();
    }
    
    @Override
    protected InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }

    @Override
    public TransactionMode getTransactionMode() {
        switch (plan.getTransactionMode()) {
        case READ_ONLY:
            return TransactionMode.READ;
        case READ_WRITE:
            return TransactionMode.WRITE;
        case NONE:
        default:
            return TransactionMode.NONE;
        }
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.ALLOWED;
    }

    @Override
    public void sendDescription(PostgresQueryContext context,
                                boolean always, boolean params)
            throws IOException {
        // The copy cases will be handled below.
        if (outputMode == DirectObjectPlan.OutputMode.TABLE)
            super.sendDescription(context, always, params);
    }

    @Override
    public boolean canSuspend(PostgresServerSession server) {
        return true;
    }

    @Override
    public DirectObjectCursor openCursor(PostgresQueryContext context, QueryBindings bindings) {
        DirectObjectCursor cursor = plan.cursor(context, bindings);
        cursor.open();
        return cursor;
    }

    public void closeCursor(DirectObjectCursor cursor) {
        if (cursor != null) {
            cursor.close();
        }
    }
    
    @Override
    public int execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        int nrows = 0;
        DirectObjectCursor cursor = null;
        PostgresOutputter<List<?>> outputter = null;
        PostgresDirectObjectCopier copier = null;
        bindings = PostgresLoadablePlan.setParameters(bindings, invocation);
        ServerCallContextStack stack = ServerCallContextStack.get();
        boolean suspended = false, success = false;
        stack.push(context, invocation);
        try {
            cursor = context.startCursor(this, bindings);
            switch (outputMode) {
            case TABLE:
                outputter = new PostgresDirectObjectOutputter(context, this);
                break;
            case COPY:
            case COPY_WITH_NEWLINE:
                outputter = copier = new PostgresDirectObjectCopier(context, this, (outputMode == DirectObjectPlan.OutputMode.COPY_WITH_NEWLINE));
                copier.respond();
                break;
            }
            if (cursor != null) {
                List<?> row;
                while ((row = cursor.next()) != null) {
                    if (row.isEmpty()) {
                        messenger.flush();
                    }
                    else {
                        outputter.output(row);
                        nrows++;
                    }
                    if ((maxrows > 0) && (nrows >= maxrows)) {
                        suspended = true;
                        break;
                    }
                }
            }
            if (copier != null) {
                copier.done();
            }
            success = true;
        }
        finally {
            suspended = context.finishCursor(this, cursor, nrows, suspended);
            stack.pop(context, invocation, success);
        }
        if (suspended) {
            messenger.beginMessage(PostgresMessages.PORTAL_SUSPENDED_TYPE.code());
            messenger.sendMessage();
        }
        else {        
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            if (copier != null)
                messenger.writeString("COPY"); // Make CopyManager happy.
            else
                messenger.writeString("CALL " + nrows);
            messenger.sendMessage();
        }
        return nrows;
    }

}
