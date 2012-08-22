/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.qp.operator;

import com.akiban.qp.exec.Plannable;
import java.util.Collections;
import java.util.List;

import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.exec.UpdateResult;
import com.akiban.qp.row.Row;
import com.akiban.server.explain.Attributes;
import com.akiban.server.explain.Explainer;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.std.DUIOperatorExplainer;
import com.akiban.util.Strings;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;
import java.util.Map;

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
 be of a UserTableRowType, and not any derived type. This can't be
 enforced by the constructor because <i>PhysicalOperator#rowType()</i>
 isn't implemented for all operators.

 <h1>Performance</h1>

 Deletion assumes the data store needs to alter the underlying storage
 system, including any system change log. This requires multiple IOs
 per operation.

 <h1>Memory Requirements</h1>

 Each row is individually processed.

 */

class Delete_Default implements UpdatePlannable {

    // constructor

    public Delete_Default(Operator inputOperator, boolean usePValues) {
        this.inputOperator = inputOperator;
        this.usePValues = usePValues;
    }

    // Object interface

    @Override
    public String toString() {
        return String.format("%s(%s)", getClass().getSimpleName(), inputOperator);
    }
    
    @Override
    public Explainer getExplainer(Map<Object, Explainer> extraInfo)
    {
        Attributes atts = new Attributes();
        if (extraInfo != null && extraInfo.containsKey(this))
        {
            atts = ((CompoundExplainer)extraInfo.get(this)).get();
        }
        return new DUIOperatorExplainer("Delete_Default", atts, inputOperator, extraInfo);
    }

    @Override
    public UpdateResult run(QueryContext context) {
        return new Execution(context, inputOperator.cursor(context)).run();
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

    private final boolean usePValues;
    private final Operator inputOperator;
    private static final InOutTap DELETE_TAP = Tap.createTimer("operator: Delete_Default");

    // Inner classes

    private class Execution extends ExecutionBase
    {
        public UpdateResult run()
        {
            int seen = 0, modified = 0;
            DELETE_TAP.in();
            try {
                input.open();
                Row oldRow;
                while ((oldRow = input.next()) != null) {
                    checkQueryCancelation();
                    ++seen;
                    adapter().deleteRow(oldRow, usePValues);
                    ++modified;
                }
            } finally {
                if (input != null) {
                    input.close();
                }
                DELETE_TAP.out();
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
