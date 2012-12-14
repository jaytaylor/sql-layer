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

import com.akiban.sql.server.ServerCallContextStack;
import com.akiban.sql.server.ServerCallInvocation;

import com.akiban.qp.loadableplan.LoadableDirectObjectPlan;
import com.akiban.qp.loadableplan.DirectObjectPlan;
import com.akiban.qp.loadableplan.DirectObjectCursor;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;

import java.util.List;
import java.io.IOException;

public class PostgresLoadableDirectObjectPlan extends PostgresDMLStatement
                                       implements PostgresQueryContext.CursorLifecycle<DirectObjectCursor>
{
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresLoadableDirectObjectPlan: execute shared");
    private static final InOutTap ACQUIRE_LOCK_TAP = Tap.createTimer("PostgresLoadableDirectObjectPlan: acquire shared lock");

    private ServerCallInvocation invocation;
    private DirectObjectPlan plan;
    private DirectObjectPlan.OutputMode outputMode;

    protected PostgresLoadableDirectObjectPlan(LoadableDirectObjectPlan loadablePlan,
                                               ServerCallInvocation invocation,
                                               List<String> columnNames, List<PostgresType> columnTypes, 
                                               PostgresType[] parameterTypes,
                                               boolean usesPValues)
    {
        super.init(null, columnNames, columnTypes, parameterTypes, usesPValues);
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
    protected InOutTap acquireLockTap()
    {
        return ACQUIRE_LOCK_TAP;
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.NONE;
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
    public void sendDescription(PostgresQueryContext context, boolean always)
            throws IOException {
        // The copy cases will be handled below.
        if (outputMode == DirectObjectPlan.OutputMode.TABLE)
            super.sendDescription(context, always);
    }

    @Override
    public DirectObjectCursor openCursor(PostgresQueryContext context) {
        DirectObjectCursor cursor = plan.cursor(context);
        cursor.open();
        return cursor;
    }

    public void closeCursor(DirectObjectCursor cursor) {
        if (cursor != null) {
            cursor.close();
        }
    }
    
    @Override
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        int nrows = 0;
        DirectObjectCursor cursor = null;
        PostgresOutputter<List<?>> outputter = null;
        PostgresDirectObjectCopier copier = null;
        context = PostgresLoadablePlan.setParameters(context, invocation, usesPValues());
        ServerCallContextStack.push(context, invocation);
        boolean suspended = false;
        try {
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
                        outputter.output(row, usesPValues());
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
        }
        finally {
            suspended = context.finishExecute(this, cursor, suspended);            
            ServerCallContextStack.pop(context, invocation);
        }
        if (suspended) {
            messenger.beginMessage(PostgresMessages.PORTAL_SUSPENDED_TYPE.code());
            messenger.sendMessage();
        }
        {        
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
