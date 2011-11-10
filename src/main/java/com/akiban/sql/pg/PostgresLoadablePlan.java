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

import com.akiban.qp.loadableplan.LoadablePlan;
import com.akiban.util.Tap;

public class PostgresLoadablePlan extends PostgresOperatorStatement
{
    protected Tap.InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }

    protected Tap.InOutTap acquireLockTap()
    {
        return ACQUIRE_LOCK_TAP;
    }

    public static PostgresLoadablePlan statement(PostgresServerSession server, String sql)
    {
        LoadablePlan loadablePlan = server.loadablePlan(sql);
        if (loadablePlan != null) {
            loadablePlan.ais(server.getAIS());
        }
        return loadablePlan == null ? null : new PostgresLoadablePlan(loadablePlan);
    }

    private PostgresLoadablePlan(LoadablePlan loadablePlan)
    {
        super(loadablePlan.plan(),
              null,
              loadablePlan.columnNames(),
              loadablePlan.columnTypes(),
              NO_INPUTS, 
              null,
              0,
              Integer.MAX_VALUE);
    }

    private static final Tap.InOutTap EXECUTE_TAP = Tap.createTimer("PostgresLoadablePlan: execute shared");
    private static final Tap.InOutTap ACQUIRE_LOCK_TAP = Tap.createTimer("PostgresLoadablePlan: acquire shared lock");
    private static final PostgresType[] NO_INPUTS = new PostgresType[0];
}
