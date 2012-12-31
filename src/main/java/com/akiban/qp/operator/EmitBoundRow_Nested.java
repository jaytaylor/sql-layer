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

import com.akiban.ais.model.UserTable;
import com.akiban.qp.exec.Plannable;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.HKeyRowType;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.explain.*;
import com.akiban.server.explain.std.LookUpOperatorExplainer;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**

 <h1>Overview</h1>

 EmitBoundRow_Nested recovers a row (or subrow) from a nested loop.

 When an <code>UPDATE</code> statement involves complex joins, the row
 to be updated is no longer an immediate input.

 <h1>Arguments</h1>

 <ul>

 <li><b>RowType inputRowType:</b> The input row that triggers the lookup.

 <li><b>RowType outputRowType:</b> The desired row type.

 <li><b>RowType boundRowType:</b> The type in the bindings.

 <li><b>int bindingPosition:</b> Indicates target row's position in the query context.

 </ul>

 <h1>Behavior</h1>
 
 The outer row is fetched from the query context for every inner row. No database access is required.

 <h1>Output</h1>

 The bound row (or a subrow).

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

  No datbase access.

 <h1>Memory Requirements</h1>

  No storage of its own.

 */

class EmitBoundRow_Nested extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s -> %s)", getClass().getSimpleName(), inputRowType, outputRowType);
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

    // EmitBoundRow_Nested interface

    public EmitBoundRow_Nested(Operator inputOperator,
                            RowType inputRowType,
                            RowType outputRowType,
                            RowType boundRowType,
                            int bindingPosition)
    {
        validateArguments(inputRowType, outputRowType, boundRowType, bindingPosition); 
        this.inputOperator = inputOperator;
        this.inputRowType = inputRowType;
        this.outputRowType = outputRowType;
        this.boundRowType = boundRowType;
        this.bindingPosition = bindingPosition;
    }

    // For use by this class

    private void validateArguments(RowType inputRowType,
                                   RowType outputRowType,
                                   RowType boundRowType,
                                   int bindingPosition)
    {
        ArgumentValidation.notNull("inputRowType", inputRowType);
        ArgumentValidation.notNull("outputRowType", outputRowType);
        ArgumentValidation.notNull("boundRowType", boundRowType);
        ArgumentValidation.isTrue("bindingPosition >= 0", bindingPosition >= 0);
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(EmitBoundRow_Nested.class);
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: EmitBoundRow_Nested open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: EmitBoundRow_Nested next");

    // Object state

    private final Operator inputOperator;
    private final RowType inputRowType, outputRowType, boundRowType;
    private final int bindingPosition;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(bindingPosition));
        atts.put(Label.OUTPUT_TYPE, outputRowType.getExplainer(context));
        return new LookUpOperatorExplainer(getName(), atts, inputRowType, false, inputOperator, context);
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
                Row row = input.next();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("EmitBoundRow: {}", row == null ? null : row);
                }
                if (row == null) {
                    close();
                }
                else  {
                    assert (row.rowType() == inputRowType);
                    Row rowFromBindings = context.getRow(bindingPosition);
                    assert (rowFromBindings.rowType() == boundRowType);
                    if (boundRowType == outputRowType) {
                        row = rowFromBindings;
                    }
                    else {
                        row = rowFromBindings.subRow(outputRowType);
                        assert (row != null) : rowFromBindings;
                    }
                }
                return row;
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close()
        {
            CursorLifecycle.checkIdleOrActive(this);
            if (input.isActive()) {
                input.close();
            }
        }

        @Override
        public void destroy()
        {
            close();
            input.destroy();
        }

        @Override
        public boolean isIdle()
        {
            return input.isIdle();
        }

        @Override
        public boolean isActive()
        {
            return input.isActive();
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
        }

        // Object state

        private final Cursor input;
    }
}
