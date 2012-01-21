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
import com.akiban.qp.loadableplan.LoadablePlan;
import com.akiban.qp.loadableplan.LoadableDirectObjectPlan;

public class PostgresLoadablePlan
{
    public static PostgresStatement statement(PostgresServerSession server, 
                                              String planName, Object[] args) {
        LoadablePlan<?> loadablePlan = server.loadablePlan(planName);
        if (loadablePlan == null)
            return null;
        loadablePlan.ais(server.getAIS());
        if (loadablePlan instanceof LoadableOperator)
            return new PostgresLoadableOperator((LoadableOperator)loadablePlan, args);
        if (loadablePlan instanceof LoadableDirectObjectPlan)
            return new PostgresLoadableDirectObjectPlan((LoadableDirectObjectPlan)loadablePlan, args);
        return null;
    }
    
    public static void setParameters(QueryContext context, Object[] args) {
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                context.setValue(i, args[i]);
            }
        }
    }

    // All static methods.
    private PostgresLoadablePlan() {
    }
}
