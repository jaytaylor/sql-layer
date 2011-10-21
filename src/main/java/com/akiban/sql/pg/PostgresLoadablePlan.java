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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.qp.loadableplan.LoadablePlan;
import com.akiban.util.Tap;

import java.io.IOException;

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

    public static PostgresLoadablePlan statement(PostgresServer server, String sql, AkibanInformationSchema ais) throws IOException
    {
        PostgresLoadablePlan statement = null;
        if (sql.startsWith(LOADABLE_PLAN_PREFIX)) {
            String className = sql.substring(LOADABLE_PLAN_PREFIX.length());
            // Get rid of trailing semicolon and, just in case, any leading or trailing whitespace.
            className = className.replace(";", "").trim();
            LoadablePlan loadablePlan = loadPlan(server, className, ais);
            statement = new PostgresLoadablePlan(loadablePlan);
        }
        return statement;
    }

    private static LoadablePlan loadPlan(PostgresServer server, String className, AkibanInformationSchema ais) throws IOException
    {
        LoadablePlan loadablePlan = server.loadablePlan(className);
        if (loadablePlan == null) {
            throw new IOException(String.format("No loadable plan registered: %s", className));
        }
        loadablePlan.ais(ais);
        return loadablePlan;
    }

    private PostgresLoadablePlan(LoadablePlan loadablePlan)
    {
        super(loadablePlan.plan(),
              null,
              loadablePlan.columnNames(),
              loadablePlan.columnTypes(),
              NO_INPUTS,
              0,
              Integer.MAX_VALUE);
    }

    private static final String LOADABLE_PLAN_PREFIX = "#run";
    private static final Tap.InOutTap EXECUTE_TAP = Tap.createTimer("PostgresLoadablePlan: execute shared");
    private static final Tap.InOutTap ACQUIRE_LOCK_TAP = Tap.createTimer("PostgresLoadablePlan: acquire shared lock");
    private static final PostgresType[] NO_INPUTS = new PostgresType[0];
}
