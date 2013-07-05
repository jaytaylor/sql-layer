/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.sql.embedded;

import com.akiban.sql.server.ServerCallInvocation;
import com.akiban.sql.server.ServerJavaRoutine;
import com.akiban.sql.server.ServerCallContextStack;

import com.akiban.qp.operator.QueryBindings;

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
    public ExecuteResults execute(EmbeddedQueryContext context, QueryBindings bindings) {
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
