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

package com.akiban.sql.embedded;

import com.akiban.sql.server.ServerRoutineInvocation;
import com.akiban.sql.server.ServerJavaRoutine;
import com.akiban.sql.server.ServerCallContextStack;

import java.sql.ResultSet;
import java.util.ArrayDeque;
import java.util.Deque;

abstract class ExecutableJavaRoutine extends ExecutableCallStatement
{
    
    protected ExecutableJavaRoutine(ServerRoutineInvocation invocation,
                                    JDBCParameterMetaData parameterMetaData) {
        super(invocation, parameterMetaData);
    }

    protected abstract ServerJavaRoutine javaRoutine(EmbeddedQueryContext context);

    @Override
    public ExecuteResults execute(EmbeddedQueryContext context) {
        ServerJavaRoutine call = javaRoutine(context);
        call.setInputs();
        ServerCallContextStack.push(context, invocation);
        try {
            call.invoke();
        }
        finally {
            ServerCallContextStack.pop(context, invocation);
        }
        call.getOutputs();

        // TODO: Get dynamic result sets.
        Deque<ResultSet> results = new ArrayDeque<ResultSet>();
        return new ExecuteResults(results);
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.ALLOWED;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }
    
}
