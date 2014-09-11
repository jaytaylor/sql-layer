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
 * LeafCursor handles the cursor processing for the Leaf operators.
 * Unlike the ChainedCursor or DualChainedCursors, the LeafCursor
 * isn't reading data from another operator cursor, but is reading it
 * from an underlying Row source. Usually this is a adapter row, or 
 * row collection. 
 * 
 * @see ChainedCursor
 * @see DualChainedCursor
 * 
 * Used By
 * @see AncestorLookup_Nested (non-lookahead)
 * @see BranchLookup_Nested
 * @see Count_TableStatus
 * @see GroupScan_Default
 * @see HKeyRow_Default
 * @see IndexScan_Default
 * @see ValuesScan_Default
 * 
 * @see IndexScan_FullText
 */
public class LeafCursor extends OperatorCursor
{
    protected final QueryBindingsCursor bindingsCursor;
    protected QueryBindings bindings;
    protected CursorLifecycle.CursorState state = CursorLifecycle.CursorState.CLOSED;

    protected LeafCursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        super(context);
        this.bindingsCursor = bindingsCursor;
    }

    @Override
    public boolean isIdle()
    {
        return state == CursorLifecycle.CursorState.IDLE;
    }

    @Override
    public boolean isActive()
    {
        return state == CursorLifecycle.CursorState.ACTIVE;
    }

    @Override
    public boolean isClosed()
    {
        return state == CursorLifecycle.CursorState.CLOSED;
    }
   
    @Override
    public void setIdle() 
    {
        state = CursorLifecycle.CursorState.IDLE;
    }
    
    @Override
    public void openBindings() {
        bindingsCursor.openBindings();
    }

    @Override
    public QueryBindings nextBindings() {
        bindings = bindingsCursor.nextBindings();
        return bindings;
    }

    @Override
    public void closeBindings() {
        bindingsCursor.closeBindings();
    }

    @Override
    public void cancelBindings(QueryBindings bindings) {
        close();
        bindingsCursor.cancelBindings(bindings);
    }
}
