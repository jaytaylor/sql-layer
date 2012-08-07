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

import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.texpressions.TEvaluatableExpression;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedExpressions;
import com.akiban.sql.optimizer.explain.*;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.InOutTap;

import java.util.*;

/**
 <h1>Overview</h1>

 Select_HKeyOrdered passes on selected rows from the input stream to the output stream. A row is subject to elimination
 if and only if it's type is a specified type (predicateType), or a descendent of this type.
 
 <h1>Arguments</h1>

 <li><b>Operator inputOperator:</b> Operator providing the input stream.
 <li><b>RowType predicateRowType:</b> Type of row to which the selection predicate is applied.
 <li><b>Expression predicate:</b> Selection predicate.
 
 <h1>Behavior</h1>
 
 The handling of a row depends on its RowType:
 
 If the row's type matches predicateRowType: The predicate is evaluated. The row is written to the output stream
 if and only if the predicate evaluates to true. 
 
 If the row's type is a descendent type of predicateRowType: The row is written to the output stream if and only if
 the predicate evaluated to true for the ancestor of type predicateRowType. (E.g., if a Customer is rejected,
 then all of its Orders and Items will be rejected too.)
 
 All other rows are written to the output stream unconditionally.

 <h1>Output</h1>

 A subset of the rows from the input stream.

 <h1>Assumptions</h1>

 Input is hkey-ordered with respect to predicateRowType. E.g., in a COI schema, with prediateRowType = Order, 
 Orders and Items are assumed to be in hkey-order. The order of one Order relative to another is not significant, nor
 is the order of Customers.

 <h1>Performance</h1>

 Project_Default does no IO. For each input row, the type is checked and each output field is computed.

 <h1>Memory Requirements</h1>

 One row of type predicateRowType.
 
 */

class Select_HKeyOrdered extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        Format f = new Format(true);
        StringBuilder sb = new StringBuilder();
        for (String row : f.Describe(this.getExplainer(null)))
        {
            sb.append(row).append('\n');
        }
        sb.setLength(sb.length()-1);
        return sb.toString();
    }

    // Operator interface


    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
    }

    @Override
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context, inputOperator.cursor(context));
    }

    @Override
    public List<Operator> getInputOperators()
    {
        List<Operator> result = new ArrayList<Operator>(1);
        result.add(inputOperator);
        return result;
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Select_HKeyOrdered interface

    public Select_HKeyOrdered(Operator inputOperator, RowType predicateRowType, Expression predicate)
    {
        this(inputOperator, predicateRowType, predicate, null);
        ArgumentValidation.notNull("predicate", predicate);
    }

    public Select_HKeyOrdered(Operator inputOperator, RowType predicateRowType, TPreparedExpression predicate)
    {
        this(inputOperator, predicateRowType, null, predicate);
        ArgumentValidation.notNull("predicate", predicate);
        if (predicate.resultType().typeClass() != AkBool.INSTANCE)
            throw new IllegalArgumentException("predicate must return type " + AkBool.INSTANCE);
    }

    private Select_HKeyOrdered(Operator inputOperator, RowType predicateRowType,
                               Expression predicate, TPreparedExpression pPredicate)
    {
        ArgumentValidation.notNull("predicateRowType", predicateRowType);
        this.inputOperator = inputOperator;
        this.predicateRowType = predicateRowType;
        this.predicate = predicate;
        this.pPredicate = pPredicate;
    }

    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Select_HKeyOrdered open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Select_HKeyOrdered next");
    
    // Object state

    private final Operator inputOperator;
    private final RowType predicateRowType;
    private final Expression predicate;
    private final TPreparedExpression pPredicate;

    @Override
    public Explainer getExplainer(Map<Object, Explainer> extraInfo)
    {
        Attributes att = new Attributes();
        
        att.put(Label.NAME, PrimitiveExplainer.getInstance("Select_HKeyOrdered"));
        att.put(Label.INPUT_OPERATOR, inputOperator.getExplainer(extraInfo));
        if (predicate != null)
            att.put(Label.PREDICATE, predicate.getExplainer(extraInfo));
        else
            att.put(Label.PREDICATE, TPreparedExpressions.getExplainer(pPredicate));
        return new OperationExplainer(Type.SELECT_HKEY, att);
    }

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                input.open();
                if (evaluation == null)
                    pEvaluation.with(context);
                else
                    evaluation.of(context);
                idle = false;
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            TAP_NEXT.in();
            try {
                CursorLifecycle.checkIdleOrActive(this);
                checkQueryCancelation();
                Row row = null;
                Row inputRow = input.next();
                while (row == null && inputRow != null) {
                    if (inputRow.rowType() == predicateRowType) {
                        if (evaluation == null) {
                            pEvaluation.with(inputRow);
                            pEvaluation.evaluate();
                            if (pEvaluation.resultValue().getBoolean(false)) {
                                // New row of predicateRowType
                                selectedRow.hold(inputRow);
                                row = inputRow;
                            }
                        }
                        else {
                            evaluation.of(inputRow);
                            if (Extractors.getBooleanExtractor().getBoolean(evaluation.eval(), false)) {
                                // New row of predicateRowType
                                selectedRow.hold(inputRow);
                                row = inputRow;
                            }
                        }
                    } else if (predicateRowType.ancestorOf(inputRow.rowType())) {
                        // Row's type is a descendent of predicateRowType.
                        if (selectedRow.isHolding() && selectedRow.get().ancestorOf(inputRow)) {
                            row = inputRow;
                        } else {
                            selectedRow.release();
                        }
                    } else {
                        row = inputRow;
                    }
                    if (row == null) {
                        inputRow = input.next();
                    }
                }
                idle = row == null;
                return row;
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close()
        {
            CursorLifecycle.checkIdleOrActive(this);
            if (!isIdle()) {
                selectedRow.release();
                input.close();
                idle = true;
            }
        }

        @Override
        public void destroy()
        {
            if (!isDestroyed()) {
                close();
                input.destroy();
                if (evaluation != null)
                    evaluation.destroy();
            }
        }

        @Override
        public boolean isIdle()
        {
            return !input.isDestroyed() && idle;
        }

        @Override
        public boolean isActive()
        {
            return !input.isDestroyed() && !idle;
        }

        @Override
        public boolean isDestroyed()
        {
            return input.isDestroyed();
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context);
            this.input = input;
            if (predicate == null) {
                this.evaluation = null;
                this.pEvaluation = pPredicate.build();
            }
            else {
                this.evaluation = predicate.evaluation();
                this.pEvaluation = null;
            }
        }

        // Object state

        private final Cursor input;
        private final ShareHolder<Row> selectedRow = new ShareHolder<Row>(); // The last input row with type = predicateRowType.
        private final ExpressionEvaluation evaluation;
        private final TEvaluatableExpression pEvaluation;
        private boolean idle = true;
    }
}
