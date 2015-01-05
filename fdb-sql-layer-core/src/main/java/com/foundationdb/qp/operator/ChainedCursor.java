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

/**
 * The first of the three complete implementations of the CursorBase
 * interface. 
 * 
 * The ChainedCursor works in the middle of the operator tree, taking
 * input from the input operator and passing along rows to the caller to next().
 * 
 * The ChainedCursor assumes nothing about the QueryBindings, 
 * in general does not use them, and only passes them along to either
 * parent or child. 
 * 
 * @See LeafCursor
 * @see MultiChainedCursor
 *
 * Used by:
 * @see Aggregate_Partial
 * @see BranchLookup_Default
 * @see Buffer_Default
 * @see Count_Default
 * @see Delete_Returning
 * @see Distinct_Partial
 * @see Filter_Default
 * @see Flatten_HKeyOrdered
 * @see GroupLookup_Default (non-lookahead)
 * @see IfEmpty_Default
 * @see Insert_Returning
 * @see Limit_Default
 * @see Product_Nested
 * @see Project_Default
 * @see Select_BloomFilter (non-lookahead)
 * @see Select_HKeyOrdered
 * @see Sort_General
 * @see Sort_InsertLimited
 * @see Update_Returning
 * @see Using_BloomFilter
 * @see Using_HashTable
 * 
 */
public class ChainedCursor extends OperatorCursor
{
    protected final Cursor input;
    protected QueryBindings bindings;
    
    protected ChainedCursor(QueryContext context, Cursor input) {
        super(context);
        this.input = input;
        if (input == null) {
            throw new NullPointerException("Input to ChainedCursor");
        }
    }

    public Cursor getInput() {
        return input;
    }

    @Override
    public void open() {
        super.open();
        input.open();
    }
    @Override
    public Row next() {
        return input.next();
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector) {
        if (CURSOR_LIFECYCLE_ENABLED) {
            CursorLifecycle.checkIdleOrActive(this);
        }
        input.jump(row, columnSelector);
        state = CursorLifecycle.CursorState.ACTIVE;
    }
    
    @Override
    public void close() {
        try {
            input.close();
        } finally {
            super.close();
        }
    }

    @Override
    public void openBindings() {
        input.openBindings();
    }

    @Override
    public QueryBindings nextBindings() {
        CursorLifecycle.checkClosed(this);
        bindings = input.nextBindings();
        return bindings;
    }

    @Override
    public void closeBindings() {
        input.closeBindings();
    }

    @Override
    public void cancelBindings(QueryBindings ancestor) {
        CursorLifecycle.checkClosed(this);
        input.cancelBindings(ancestor);
    }
}
