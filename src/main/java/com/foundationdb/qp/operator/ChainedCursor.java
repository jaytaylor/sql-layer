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

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;

public class ChainedCursor extends OperatorCursor
{
    protected final Cursor input;
    protected QueryBindings bindings;

    protected ChainedCursor(QueryContext context, Cursor input) {
        super(context);
        this.input = input;
    }

    public Cursor getInput() {
        return input;
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

    @Override
    public void openBindings() {
        input.openBindings();
    }

    @Override
    public QueryBindings nextBindings() {
        bindings = input.nextBindings();
        return bindings;
    }

    @Override
    public void closeBindings() {
        input.closeBindings();
    }

    @Override
    public void cancelBindings(QueryBindings ancestor) {
        input.cancelBindings(ancestor);
        close();                // In case override maintains some additional state.
    }
}
