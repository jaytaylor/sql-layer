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
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.PrimitiveExplainer;
import com.akiban.sql.optimizer.explain.std.SortOperatorExplainer;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.tap.InOutTap;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 <h1>Overview</h1>

 Sort_Tree generates an output stream containing all the rows of the input stream, sorted according to an
 ordering specification. The "Tree" in the name refers to the implementation, in which the rows are inserted
 into a B-tree (presumably on-disk) and then read out in order.

 <h1>Arguments</h1>

 <li><b>Operator inputOperator:</b> Operator providing the input stream.
 <li><b>RowType sortType:</b> Type of rows to be sorted.
 <li><b>API.Ordering ordering:</b> Specification of ordering, comprising a list of expressions and ascending/descending
 specifications.
 <li><b>API.SortOption sortOption:</b> Specifies whether duplicates should be kept (PRESERVE_DUPLICATES) or eliminated
 (SUPPRESS_DUPLICATES)

 <h1>Behavior</h1>

 The rows of the input stream are written into a B-tree that orders rows according to the ordering specification.
 Once the input stream has been consumed, the B-tree is traversed from beginning to end to provide rows of the output
 stream.

 <h1>Output</h1>

 The rows of the input stream, sorted according to the ordering specification. Duplicates are eliminated if
 and only if the sortOption is SUPPRESS_DUPLICATES.

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 Sort_Tree generates IO dependent on the size of the input stream. This occurs mostly during the loading phase,
 (when the input stream is being read). There will be some IO when the loaded B-tree is scanned, but this is
 expected to be more efficient, as each page will be read completely before moving on to the next one.

 <h1>Memory Requirements</h1>

 Memory requirements (and disk requirements) depend on the underlying B-tree.

 */

class Sort_Tree extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        if (sortOption == API.SortOption.PRESERVE_DUPLICATES)
            return String.format("%s(%s)", getClass().getSimpleName(), sortType);
        else
            return String.format("%s(%s, %s)", getClass().getSimpleName(), sortType, sortOption.name());
    }

    // Operator interface

    @Override
    public List<Operator> getInputOperators()
    {
        return Collections.singletonList(inputOperator);
    }

    @Override
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context, inputOperator.cursor(context));
    }

    @Override
    public RowType rowType()
    {
        return sortType;
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
        derivedTypes.add(sortType);
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Sort_Tree interface

    public Sort_Tree(Operator inputOperator,
                     RowType sortType,
                     API.Ordering ordering,
                     API.SortOption sortOption,
                     boolean usePValues)
    {
        ArgumentValidation.notNull("sortType", sortType);
        ArgumentValidation.isGT("ordering.columns()", ordering.sortColumns(), 0);
        this.inputOperator = inputOperator;
        this.sortType = sortType;
        this.ordering = ordering;
        this.sortOption = sortOption;
        this.usePValues = usePValues;
    }
    
    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Sort_Tree open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Sort_Tree next");
    private static final InOutTap TAP_LOAD = OPERATOR_TAP.createSubsidiaryTap("operator: Sort_Tree load");

    // Object state

    private final Operator inputOperator;
    private final RowType sortType;
    private final API.Ordering ordering;
    private final API.SortOption sortOption;
    private final boolean usePValues;

    @Override
    public Explainer getExplainer(Map extraInfo)
    {
        return new SortOperatorExplainer("Sort_Tree", sortOption, sortType, inputOperator, ordering, extraInfo);
        
        // TODO implement for real
        //return PrimitiveExplainer.getInstance(toString()); // Dummy explainer
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
                output = adapter().sort(context, input, sortType, ordering, sortOption, TAP_LOAD, usePValues);
                output.open();
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            Row row = null;
            TAP_NEXT.in();
            try {
                CursorLifecycle.checkIdleOrActive(this);
                checkQueryCancelation();
                if (!input.isActive()) {
                    row = output.next();
                    if (row == null) {
                        close();
                    }
                }
            } finally {
                TAP_NEXT.out();
            }
            return row;
        }

        @Override
        public void close()
        {
            CursorLifecycle.checkIdleOrActive(this);
            if (output != null) {
                input.close();
                output.close();
                output = null;
            }
        }

        @Override
        public void destroy()
        {
            close();
            input.destroy();
            if (output != null) {
                output.destroy();
                output = null;
            }
            destroyed = true;
        }

        @Override
        public boolean isIdle()
        {
            return !destroyed && output == null;
        }

        @Override
        public boolean isActive()
        {
            return !destroyed && output != null;
        }

        @Override
        public boolean isDestroyed()
        {
            return destroyed;
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context);
            this.input = input;
        }

        // Object state

        private final Cursor input;
        private Cursor output;
        private boolean destroyed = false;
    }
}
