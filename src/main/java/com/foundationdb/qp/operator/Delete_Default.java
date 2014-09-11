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

import java.util.Collections;
import java.util.List;

import com.foundationdb.qp.exec.UpdatePlannable;
import com.foundationdb.qp.exec.UpdateResult;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.explain.Attributes;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.std.DUIOperatorExplainer;
import com.foundationdb.util.Strings;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**

 <h1>Overview</h1>

 The Delete_Default deletes rows from a given table. Every row provided
 by the input operator is sent to the <i>StoreAdapter#deleteRow()</i>
 method to be removed from the table.

 <h1>Arguments</h1>

 <ul>

 <li><b>input:</b> the input operator supplying rows to be deleted.

 </ul>

 <h1>Behaviour</h1>

 Rows supplied by the input operator are deleted from the underlying
 data store through the StoreAdapter interface.

 <h1>Output</h1>

 The operator does not create a cursor returning rows. Instead it
 supplies a run() method which returns an <i>UpdateResult</i>

 <h1>Assumptions</h1>

 The rows provided by the input operator includes all of the columns
 for the HKEY to allow the persistit layer to lookup the row in the
 btree to remove it. Failure results in a RowNotFoundException being
 thrown and the operation aborted.

 The operator assumes (but does not require) that all rows provided are
 of the same RowType.

 The Delete_Default operator assumes (and requires) the input row types
 be of a TableRowType, and not any derived type. This can't be
 enforced by the constructor because <i>PhysicalOperator#rowType()</i>
 isn't implemented for all operators.

 <h1>Performance</h1>

 Deletion assumes the data store needs to alter the underlying storage
 system, including any system change log. This requires multiple IOs
 per operation.

 <h1>Memory Requirements</h1>

 Each row is individually processed.

 */
@Deprecated
class Delete_Default implements UpdatePlannable {

    // constructor

    public Delete_Default(Operator inputOperator) {
        this.inputOperator = inputOperator;
    }

    // Object interface

    @Override
    public String toString() {
        return String.format("%s(%s)", getName(), inputOperator);
    }
    
    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        if (context.hasExtraInfo(this))
            atts.putAll(context.getExtraInfo(this).get());
        return new DUIOperatorExplainer(getName(), atts, inputOperator, context);
    }

    @Override
    public UpdateResult run(QueryContext context, QueryBindings bindings) {
        QueryBindingsCursor bindingsCursor = new SingletonQueryBindingsCursor(bindings);
        return new Execution(context, inputOperator.cursor(context, bindingsCursor)).run();
    }

    @Override
    public String describePlan() {
        return describePlan(inputOperator);
    }

    @Override
    public String describePlan(Operator inputOperator) {
        return inputOperator + Strings.nl() + this;
    }

    @Override
    public List<Operator> getInputOperators() {
        return Collections.singletonList(inputOperator);
    }

    private final Operator inputOperator;
    private static final InOutTap DELETE_TAP = Tap.createTimer("operator: Delete_Default");
    private static final Logger LOG = LoggerFactory.getLogger(Delete_Default.class);

    // Inner classes

    private class Execution extends ExecutionBase
    {
        public UpdateResult run()
        {
            int seen = 0, modified = 0;
            if (TAP_NEXT_ENABLED) {
                DELETE_TAP.in();
            }
            try {
                input.openTopLevel();
                Row oldRow;
                while ((oldRow = input.next()) != null) {
                    checkQueryCancelation();
                    if (LOG_EXECUTION) {
                        LOG.debug("Delete_Default: deleting {}", oldRow);
                    }
                    ++seen;
                    adapter().deleteRow(oldRow, false);
                    ++modified;
                }
            } finally {
                if (input != null) {
                    input.close();
                }
                if (TAP_NEXT_ENABLED) {
                    DELETE_TAP.out();
                }
            }
            return new StandardUpdateResult(seen, modified);
        }

        protected Execution(QueryContext queryContext, Cursor input)
        {
            super(queryContext);
            this.input = input;
        }

        private final Cursor input;
    }
}
