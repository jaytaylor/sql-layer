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

import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerCallInvocation;

import com.foundationdb.ais.model.Column;
import com.foundationdb.qp.loadableplan.LoadableOperator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;

import java.io.IOException;
import java.util.List;

public class PostgresLoadableOperator extends PostgresOperatorStatement
{
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresLoadableOperator: execute shared");

    private ServerCallInvocation invocation;

    protected PostgresLoadableOperator(LoadableOperator loadableOperator, 
                                       ServerCallInvocation invocation,
                                       List<String> columnNames, List<PostgresType> columnTypes, List<Column> aisColumns,
                                       PostgresType[] parameterTypes)
    {
        super(null);
        super.init(loadableOperator.plan(), null, columnNames, columnTypes, aisColumns, parameterTypes, null);
        this.invocation = invocation;
    }
    
    @Override
    protected InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }

    @Override
    public PostgresStatementResult execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        bindings = PostgresLoadablePlan.setParameters(bindings, invocation);
        ServerCallContextStack stack = ServerCallContextStack.get();
        boolean success = false;
        stack.push(context, invocation);
        try {
            PostgresStatementResult result = super.execute(context, bindings, maxrows);
            success = true;
            return result;
        }
        finally {
            stack.pop(context, invocation, success);
        }
    }

}
