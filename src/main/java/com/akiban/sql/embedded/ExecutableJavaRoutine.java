
package com.akiban.sql.embedded;

import com.akiban.sql.server.ServerCallInvocation;
import com.akiban.sql.server.ServerJavaRoutine;
import com.akiban.sql.server.ServerCallContextStack;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Queue;

abstract class ExecutableJavaRoutine extends ExecutableCallStatement
{
    
    protected ExecutableJavaRoutine(ServerCallInvocation invocation,
                                    JDBCParameterMetaData parameterMetaData) {
        super(invocation, parameterMetaData);
    }

    protected abstract ServerJavaRoutine javaRoutine(EmbeddedQueryContext context);

    @Override
    public ExecuteResults execute(EmbeddedQueryContext context) {
        Queue<ResultSet> resultSets = null;
        ServerJavaRoutine call = javaRoutine(context);
        call.push();
        boolean success = false;
        try {
            call.setInputs();
            call.invoke();
            resultSets = call.getDynamicResultSets();
            call.getOutputs();
            success = true;
        }
        finally {
            if ((resultSets != null) && !success) {
                while (!resultSets.isEmpty()) {
                    try {
                        resultSets.remove().close();
                    }
                    catch (SQLException ex) {
                    }
                }
            }
            call.pop(success);
        }
        return new ExecuteResults(resultSets);
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.ALLOWED;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.NOT_ALLOWED;
    }
}
