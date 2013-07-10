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

package com.akiban.qp.operator;

import com.akiban.qp.row.Row;
import com.akiban.server.api.dml.ColumnSelector;

public class ChainedCursor extends OperatorExecutionBase implements Cursor
{
    protected final Cursor input;

    protected ChainedCursor(QueryContext context, QueryBindings bindings, Cursor input) {
        super(context, bindings);
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
    public void jump(Row row, ColumnSelector columnSelector)
    {
        input.jump(row, columnSelector);
    }

    @Override
    public void close() {
        input.close();
    }

    @Override
    public void destroy()
    {
        input.destroy();
    }

    @Override
    public boolean isIdle()
    {
        return input.isIdle();
    }

    @Override
    public boolean isActive()
    {
        return input.isActive();
    }

    @Override
    public boolean isDestroyed()
    {
        return input.isDestroyed();
    }
}
