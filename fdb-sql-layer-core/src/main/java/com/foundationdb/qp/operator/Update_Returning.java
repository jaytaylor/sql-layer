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
import com.foundationdb.server.explain.Attributes;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.explain.std.DUIOperatorExplainer;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**

<h1>Overview</h1>

Provides row update functionality.

<h1>Arguments</h1>

<ul>

<li><b>PhysicalOperator inputOperator:</b> Provides rows to be updated

<li><b>UpdateFunction updateFunction:</b> specifies which rows are to be updated, and how

</ul>

<h1>Behavior</h1>

For each row from the input operator's cursor, it invokes <i>updateFunction.evaluate</i> to
get the new version of the row. It then performs the update in an
unspecified way (in practice, this is currently done via
<i>StoreAdapater.updateRow</i>, which is implemented by
<i>PersistitAdapater.updateRow</i>, which invokes
<i>PersistitStore.updateRow</i>).

The updated row is then returned to the caller as with other operators. 

<h1>Output</h1>

The row as modified by the updateFunction().

<h1>Assumptions</h1>

Selected rows must have a <i>RowType</i> such
that <i>rowType.hasTable() == true</i>.

<h1>Performance</h1>

Updating rows may be slow, especially since indexes are also
updated. There are several random-access reads and writes involved,
which depend on the indexes defined for that row type.

There are potentially ways to optimize this, if we can
push <i>WHERE</i> clauses down; this would mean we could update some
indexes as a batch operation, rather than one at a time. This would
require changes to the API, and is not currently a priority.

<h1>Memory Requirements</h1>

Each <i>UpdateFunction.evaluate</i> method may generate a
new <i>Row</i>.

*/

public class Update_Returning extends Operator {

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes atts = new Attributes();
        if (context.hasExtraInfo(this))
            atts.putAll(context.getExtraInfo(this).get()); 
        atts.put(Label.EXTRA_TAG, PrimitiveExplainer.getInstance(updateFunction.toString()));
        return new DUIOperatorExplainer(getName(), atts, inputOperator, context);
    }

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        return new Execution(context, inputOperator.cursor(context, bindingsCursor));
    }
    
    public Update_Returning (Operator inputOperator, UpdateFunction updateFunction) {
        ArgumentValidation.notNull("update lambda", updateFunction);
        
        this.inputOperator = inputOperator;
        this.updateFunction = updateFunction;
    }

    @Override
    public String toString() {
        return String.format("%s(%s -> %s)", getName(), inputOperator, updateFunction);
    }

    private final Operator inputOperator;
    private final UpdateFunction updateFunction;
    // Class state
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: UpdateReturning open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: UpdateReturning next");
    private static final Logger LOG = LoggerFactory.getLogger(Update_Returning.class);

    // Inner classes
    private class Execution extends ChainedCursor
    {

        // Cursor interface
    
        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                super.open();
            } finally {
                TAP_OPEN.out();
            }
        }
    
        @Override
        public Row next()
        {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                if (CURSOR_LIFECYCLE_ENABLED) {
                    CursorLifecycle.checkIdleOrActive(this);
                }
                checkQueryCancelation();
                
                Row inputRow;
                Row newRow = null;
                if ((inputRow = input.next()) != null) {
                    newRow = updateFunction.evaluate(inputRow, context, bindings);
                    adapter().updateRow(inputRow, newRow);
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Update_Returning: updating {} to {}", inputRow, newRow);
                }
                return newRow; 
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }
        // Execution interface
    
        Execution(QueryContext context, Cursor input)
        {
            super(context, input);
        }
    
        // Object state
    }

}
