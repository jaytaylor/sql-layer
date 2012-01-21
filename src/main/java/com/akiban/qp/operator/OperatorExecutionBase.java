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

package com.akiban.qp.operator;

public abstract class OperatorExecutionBase
{
    // Operators that implement cursors have a context at construction time
    protected OperatorExecutionBase(QueryContext context)
    {
        this.context = context;
    }

    // Update operators don't get the context until later
    protected OperatorExecutionBase()
    {
    }

    protected void context(QueryContext context)
    {
        this.context = context;
    }

    protected void checkQueryCancelation()
    {
        adapter().checkQueryCancelation(context.getStartTime());
    }

   protected StoreAdapter adapter()
    {
        return context.getStore();
    }

    protected QueryContext context;
}
