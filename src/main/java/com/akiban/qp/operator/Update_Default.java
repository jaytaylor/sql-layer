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

import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.exec.UpdateResult;
import com.akiban.qp.row.Row;
import com.akiban.server.explain.*;
import com.akiban.server.explain.std.DUIOperatorExplainer;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.Strings;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**

 <h1>Overview</h1>

 Provides row update functionality.

 <h1>Arguments</h1>

 <ul>

 <li><b>PhysicalOperator inputOperator:</b> Provides rows to be updated

 <li><b>UpdateFunction updateFunction:</b> specifies which rows are to be updated, and how

 </ul>

 <h1>Behavior</h1>

 For each row from the input operator's cursor, the UpdateOperator
 invokes <i>updateFunction.rowIsSelected</i> to determine if the row
 should be updated. If so, it invokes <i>updateFunction.evaluate</i> to
 get the new version of the row. It then performs the update in an
 unspecified way (in practice, this is currently done via
 <i>StoreAdapater.updateRow</i>, which is implemented by
 <i>PersistitAdapater.updateRow</i>, which invokes
 <i>PersistitStore.updateRow</i>).

 The result of this update is an <i>UpdaateResult</i> instance which summarizes how many rows were updated and how long the operation took.

 <h1>Output</h1>

 N/A (this is an UpdatePlannable, not an Operator).

 <h1>Assumptions</h1>

 Selected rows must have a <i>RowType</i> such
 that <i>rowType.hasUserTable() == true</i>.

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

class Update_Default implements UpdatePlannable {

    // Object interface

    @Override
    public String toString() {
        return String.format("%s(%s -> %s)", getClass().getSimpleName(), inputOperator, updateFunction);
    }

    // constructor

    public Update_Default(Operator inputOperator, UpdateFunction updateFunction) {
        ArgumentValidation.notNull("update lambda", updateFunction);
        
        this.inputOperator = inputOperator;
        this.updateFunction = updateFunction;
    }

    // UpdatePlannable interface

    @Override
    public UpdateResult run(QueryContext context) {
        return new Execution(context, inputOperator.cursor(context)).run();
    }

    // Plannable interface

    @Override
    public List<Operator> getInputOperators() {
        return Collections.singletonList(inputOperator);
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    @Override
    public String describePlan(Operator inputOperator) {
        return inputOperator + Strings.nl() + this;
    }

    // Object state

    private final Operator inputOperator;
    private final UpdateFunction updateFunction;
    private static final InOutTap UPDATE_TAP = Tap.createTimer("operator: Update_Default");

    @Override
    public Explainer getExplainer(Map<Object, Explainer> extraInfo)
    {
        Attributes atts = new Attributes();
        if (extraInfo != null && extraInfo.containsKey(this))
        {
            atts = ((OperationExplainer)extraInfo.get(this)).get();
        }
        atts.put(Label.EXTRA_TAG, PrimitiveExplainer.getInstance(updateFunction.toString()));
        OperationExplainer ex = new DUIOperatorExplainer("Update_Default", atts, inputOperator, extraInfo);
        return ex;
    }

    // Inner classes

    private class Execution extends ExecutionBase
    {
        public UpdateResult run()
        {
            boolean usePValues = updateFunction.usePValues();
            int seen = 0, modified = 0;
            UPDATE_TAP.in();
            try {
                input.open();
                Row oldRow;
                while ((oldRow = input.next()) != null) {
                    checkQueryCancelation();
                    ++seen;
                    if (updateFunction.rowIsSelected(oldRow)) {
                        Row newRow = updateFunction.evaluate(oldRow, context);
                        context.checkConstraints(newRow, usePValues);
                        adapter().updateRow(oldRow, newRow, usePValues);
                        ++modified;
                    }
                }
            } finally {
                if (input != null) {
                    input.close();
                }
                UPDATE_TAP.out();
            }
            return new StandardUpdateResult(seen, modified);
        }

        public Execution(QueryContext queryContext, Cursor input)
        {
            super(queryContext);
            this.input = input;
        }

        private final Cursor input;
    }
}
