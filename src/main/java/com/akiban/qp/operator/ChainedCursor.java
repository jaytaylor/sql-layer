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

import com.akiban.qp.row.Row;

public abstract class ChainedCursor extends OperatorExecutionBase implements Cursor
{
    protected final Cursor input;

    protected ChainedCursor(QueryContext context, Cursor input) {
        super(context);
        this.input = input;
    }

    @Override
    public void open() {
        input.open();
    }

    @Override
    public Row next()
    {
        return input.next();
    }

    @Override
    public void close() {
        input.close();
    }
}
