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
 * @see MultiChainedCursor
 * 
 * Used By
 * @see AncestorLookup_Nested$Execution (non-lookahead)
 * @see BranchLookup_Nested$Execution
 * @see Count_TableStatus$Execution
 * @see GroupScan_Default$Execution
 * @see HKeyRow_Default$Execution
 * @see IndexScan_Default$Execution
 * @see ValuesScan_Default$Execution
 * 
 * @see com.foundationdb.server.service.text.IndexScan_FullText$Execution
 * @see com.foundationdb.server.test.it.qp.QueryTimeoutIT$DoNothingForever$Execution
 */
public class LeafCursor extends OperatorCursor
{
    protected final QueryBindingsCursor bindingsCursor;
    protected QueryBindings bindings;

    protected LeafCursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        super(context);
        this.bindingsCursor = bindingsCursor;
    }

    @Override
    public void openBindings() {
        bindingsCursor.openBindings();
    }

    @Override
    public QueryBindings nextBindings() {
        CursorLifecycle.checkClosed(this);
        bindings = bindingsCursor.nextBindings();
        return bindings;
    }

    @Override
    public void closeBindings() {
        bindingsCursor.closeBindings();
    }

    @Override
    public void cancelBindings(QueryBindings bindings) {
        CursorLifecycle.checkClosed(this);
        bindingsCursor.cancelBindings(bindings);
    }
}
