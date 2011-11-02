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
    public long startTimeMsec()
    {
        return startTimeMsec;
    }

    // Operators that implement cursors have an adapter at construction time
    protected OperatorExecutionBase(StoreAdapter adapter)
    {
        this();
        this.adapter = adapter;
    }

    // Update operators don't get the adapter until later
    protected OperatorExecutionBase()
    {
        this.startTimeMsec = System.currentTimeMillis();
    }

    protected void adapter(StoreAdapter adapter)
    {
        this.adapter = adapter;
    }

    protected void checkQueryCancelation()
    {
        adapter.checkQueryCancelation(startTimeMsec);
    }

    protected StoreAdapter adapter;
    // startTimeMsec is used to control query timeouts. There is no central place in which a query's start time
    // is recorded. Instead, each operator's cursor in a plan records System.currentTimeMillis(). This should
    // be good enough for query timeouts unless some operator cursors are created much later (e.g. seconds) than others
    // in the same query.
    private final long startTimeMsec;
}
