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

package com.foundationdb.qp.operator;

import com.foundationdb.ais.model.Table;

public abstract class ExecutionBase
{
    protected StoreAdapter adapter()
    {
        return context.getStore();
    }

    protected StoreAdapter adapter(Table name)
    {
        return context.getStore(name);
    }

    protected void checkQueryCancelation()
    {
        context.checkQueryCancelation();
    }

    public ExecutionBase(QueryContext context)
    {
        this.context = context;
    }

    protected QueryContext context;

    protected static final boolean LOG_EXECUTION = false;
    protected static final boolean TAP_NEXT_ENABLED = false;
    public static final boolean CURSOR_LIFECYCLE_ENABLED = false;
}
