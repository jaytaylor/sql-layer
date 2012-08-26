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
import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.exec.UpdateResult;
import com.akiban.qp.row.Row;
import com.akiban.server.explain.*;
import com.akiban.server.explain.std.DUIOperatorExplainer;
import com.akiban.util.Strings;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**

 <h1>Overview</h1>

 Inserts new rows into a table. This is an UpdatePlannable, not a PhysicalOperator.

 <h1>Arguments</h1>

 <ul>

 <li><b>PhysicalOperator inputOperator:</b> source of rows to be inserted

 </ul>

 <h1>Behaviour</h1>

 For each row in the insert operator, the row in inserted into the
 table. In practice, this is currently done via
 <i>StoreAdapater#insertRow</i>, which is implemented by
 <i>PersistitAdapater#insertRow</i>, which invokes
 <i>PersistitStore#insertRow</i>

 The result of this update is an UpdateResult instance which summarizes
 how many rows were updated and how long the operation took.

 <h1>Output</h1>

 N/A

 <h1>Assumptions</h1>

 The inputOperator is returning rows of the UserTableRowType of the table being inserted into.

 <h1>Performance</h1>

 Insert may be slow because because indexes are also updated. Insert
 may be able to be improved in performance by batching the index
 updates, but there is no current API to so.

 <h1>Memory Requirements</h1>

 None.

 */

class Insert_Default implements UpdatePlannable {

    public Insert_Default(Operator inputOperator, boolean usePValues) {
        this.inputOperator = inputOperator;
        this.usePValues = usePValues;
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

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", getName(), inputOperator);
    }

    private final Operator inputOperator;
    private final boolean usePValues;
    private static final InOutTap INSERT_TAP = Tap.createTimer("operator: Insert_Default");
    private static final Logger LOG = LoggerFactory.getLogger(Insert_Default.class);

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        if (context.hasExtraInfo(this))
            atts.putAll(context.getExtraInfo(this).get()); 
        return new DUIOperatorExplainer(getName(), atts, inputOperator, context);
    }

    // Inner classes

    private class Execution extends ExecutionBase
    {
        public UpdateResult run()
        {
            int seen = 0, modified = 0;
            INSERT_TAP.in();
            try {
                input.open();
                Row row;
                while ((row = input.next()) != null) {
                    // LOG.warn("About to insert {}: {}", row.rowType().userTable(), row);
                    checkQueryCancelation();
                    ++seen;
                    context.checkConstraints(row, usePValues);
                    adapter().writeRow(row, usePValues);
                    ++modified;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Insert: row {}", row);
                    }
                }
            } finally {
                if (input != null) {
                    input.close();
                }
                INSERT_TAP.out();
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
