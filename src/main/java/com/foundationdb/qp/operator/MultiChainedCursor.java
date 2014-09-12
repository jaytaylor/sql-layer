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

/**
 * The third of the three complete implementations of the CursorBase
 * interface. 
 * 
 * The MultiChainedCursor works in the middle of the operator tree, taking
 * input from two input operators and passing the resulting aggregation
 * through next()
 * 
 * The MultiChainedCursor assumes nothing about the QueryBindings, 
 * in general does not use them, and only passes them along to either
 * parent or child. 
 * 
 * @See LeafCursor
 * @see ChainedCursor
 *
 * Used by:
 * @see Except_Ordered$Execution
 * @see HKeyUnion_Ordered$Execution
 * @see Intersect_Ordered$Execution
 * @see Union_Ordered$Execution
 * @see UnionAll_Default$Execution
 */
public abstract class MultiChainedCursor extends  OperatorCursor {

    protected Cursor leftInput;
    protected Cursor rightInput;
    protected final QueryBindingsCursor bindingsCursor;
    
    protected MultiChainedCursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        super(context);
        MultipleQueryBindingsCursor multiple = new MultipleQueryBindingsCursor(bindingsCursor);
        this.bindingsCursor = multiple;
        this.leftInput = left().cursor(context, multiple.newCursor());
        this.rightInput = right().cursor(context, multiple.newCursor());
    }
    
    protected abstract Operator left();
    protected abstract Operator right();

    @Override
    public void open() {
        CursorLifecycle.checkClosed(leftInput);
        CursorLifecycle.checkClosed(rightInput);
        leftInput.open();
        rightInput.open();
        super.open();
    }

    @Override
    public void close() {
        if (CURSOR_LIFECYCLE_ENABLED) {
            CursorLifecycle.checkIdleOrActive(leftInput);
            CursorLifecycle.checkIdleOrActive(rightInput);
        }
        leftInput.close();
        rightInput.close();
        super.close();
    }

    @Override
    public void openBindings() {
        bindingsCursor.openBindings();
        leftInput.openBindings();
        rightInput.openBindings();
    }

    @Override
    public QueryBindings nextBindings() {
        QueryBindings bindings = bindingsCursor.nextBindings();
        QueryBindings other = leftInput.nextBindings();
        assert (bindings == other);
        other = rightInput.nextBindings();
        assert (bindings == other);
        return bindings;
    }

    @Override
    public void closeBindings() {
        bindingsCursor.closeBindings();
        leftInput.closeBindings();
        rightInput.closeBindings();
    }

    @Override
    public void cancelBindings(QueryBindings bindings) {
        CursorLifecycle.checkClosed(this);
        //close();                // In case override maintains some additional state.
        leftInput.cancelBindings(bindings);
        rightInput.cancelBindings(bindings);
        bindingsCursor.cancelBindings(bindings);
    }
}
