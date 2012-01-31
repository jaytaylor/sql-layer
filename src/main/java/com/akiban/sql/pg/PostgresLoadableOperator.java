/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.pg;

import com.akiban.qp.loadableplan.LoadableOperator;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;

import java.io.IOException;

public class PostgresLoadableOperator extends PostgresOperatorStatement
{
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresLoadableOperator: execute shared");
    private static final InOutTap ACQUIRE_LOCK_TAP = Tap.createTimer("PostgresLoadableOperator: acquire shared lock");

    private Object[] args;

    protected PostgresLoadableOperator(LoadableOperator loadableOperator, Object[] args)
    {
        super(loadableOperator.plan(),
              null,
              loadableOperator.columnNames(),
              loadableOperator.columnTypes(),
              null);
        this.args = args;
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
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        // Overwrite the query parameters with the call parameters.
        PostgresLoadablePlan.setParameters(context, args);
        return super.execute(context, maxrows);
    }

}
