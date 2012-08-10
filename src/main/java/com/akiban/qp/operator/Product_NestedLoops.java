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

import com.akiban.qp.row.ProductRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.ProductRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.std.NestedLoopsExplainer;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**

 <h1>Overview</h1>

 Product_NestedLoops computes the cartesian product of child rows owned by the same parent row. For example, if customer c1 has orders o10 and o11, and addresses a10 and a11; and customer c2 has order o20 and address a20, then Product_NestedLoops computes \{o10, o11\} x \{a10, a11\} for c1, and \{o20\} x \{a20\} for c2.

 <h1>Arguments</h1>

 <ul>

 <li><b>RowType outerType:</b> Type of one child row. parent rows to be
 flattened.

 <li><b>UserTableRowType branchType:</b> Ancestor type of outerType and innerType. Output will consist
 of the cartesian product of outer and inner rows that match when projected to the branch type.

 <li><b>RowType innerType:</b> Type of the other child row.

 <li><b>int inputBindingPosition:</b> The position in the bindings that
 will be used to pass rows from the outer loop to the inner loop.

 </ul>

 The input types must have resulted from Flattens of some common
 ancestor, e.g. Flatten(customer, order) and Flatten(customer,
 address).

 <h1>Behavior</h1>

 Suppose we have a COA schema (C is the parent of O and A), and that we
 have flattened C with O, and C with A. Product_NestedLoops has two
 input streams, with CO in one and CA in the other. The branch type is C.
 For this discussion, let's assume that CO is the outer stream, and CA is in the
 inner stream. The terms "outer" and "inner" reflect the relative
 positions of the inputs in the nested loops used to compute the
 product of the inputs.

 For each CO row from the outer input stream, a set of matching CA rows,
 (i.e., matching in C)
 from the inner input stream are retrieved. The CO row and all of the
 CA rows will have the same customer primary key. Product_NestedLoops
 will write to the output stream product rows for each CO/CA
 combination.

 <h1>Output</h1>

 For CO/CA input streams, the format of an output row is (C, O, A),
 i.e., all the customer fields, followed by all the order fields,
 followed by all the address fields.

 Product rows do not have an hkey.

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 Product_NestedLoops does no IO.

 <h1>Memory Requirements</h1>

 For each outer row, all matching rows from the inner input are kept in
 memory. This avoids the need to retrieve the inner rows repeatedly in
 situations where consecutive outer rows match the same inner rows,
 e.g. for hkey-ordered outer rows.

 */

class Product_NestedLoops extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s x %s)", getClass().getSimpleName(), outerType, innerType);
    }

    @Override
    public Explainer getExplainer(Map<Object, Explainer> extraInfo)
    {
        return new NestedLoopsExplainer("Product_NestedLoops", innerInputOperator, outerInputOperator, innerType, outerType, extraInfo);
    }
    
    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context);
    }

    @Override
    public ProductRowType rowType()
    {
        return productType;
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        innerInputOperator.findDerivedTypes(derivedTypes);
        outerInputOperator.findDerivedTypes(derivedTypes);
        derivedTypes.add(productType);
    }

    @Override
    public List<Operator> getInputOperators()
    {
        List<Operator> result = new ArrayList<Operator>(2);
        result.add(outerInputOperator);
        result.add(innerInputOperator);
        return result;
    }

    @Override
    public String describePlan()
    {
        return describePlan(outerInputOperator);
    }

    // Project_Default interface

    public Product_NestedLoops(Operator outerInputOperator,
                               Operator innerInputOperator,
                               RowType outerType,
                               UserTableRowType branchType,
                               RowType innerType,
                               int inputBindingPosition)
    {
        ArgumentValidation.notNull("outerInputOperator", outerInputOperator);
        ArgumentValidation.notNull("innerInputOperator", innerInputOperator);
        ArgumentValidation.notNull("outerType", outerType);
        ArgumentValidation.notNull("innerType", innerType);
        ArgumentValidation.isGTE("inputBindingPosition", inputBindingPosition, 0);
        this.outerInputOperator = outerInputOperator;
        this.innerInputOperator = innerInputOperator;
        this.outerType = outerType;
        this.innerType = innerType;
        this.productType = outerType.schema().newProductType(outerType, branchType, innerType);
        this.branchType = productType.branchType();
        this.inputBindingPosition = inputBindingPosition;
    }

    // Class state

    private static InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Product_NestedLoops open");
    private static InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Product_NestedLoops next");
    private static final Logger LOG = LoggerFactory.getLogger(BranchLookup_Nested.class);

    // Object state

    private final Operator outerInputOperator;
    private final Operator innerInputOperator;
    private final RowType branchType;
    private final RowType outerType;
    private final RowType innerType;
    private final ProductRowType productType;
    private final int inputBindingPosition;

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
                this.outerInput.open();
                this.closed = false;
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
                Row outputRow = null;
                while (!closed && outputRow == null) {
                    outputRow = nextProductRow();
                    if (outputRow == null) {
                        Row row = outerInput.next();
                        if (row == null) {
                            close();
                        } else {
                            RowType rowType = row.rowType();
                            if (rowType == outerType) {
                                Row branchRow = row.subRow(branchType);
                                assert branchRow != null : row;
                                if (outerBranchRow.isEmpty() || !branchRow.hKey().equals(outerBranchRow.get().hKey())) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("Product_NestedLoops: branch row {}", row);
                                    }
                                    outerBranchRow.hold(branchRow);
                                    innerRows.newBranchRow(branchRow);
                                }
                                outerRow.hold(row);
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Product_NestedLoops: restart inner loop using current branch row");
                                }
                                innerRows.resetForCurrentBranchRow();
                            }
                        }
                    }
                }
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Product_NestedLoops: yield {}", outputRow);
                }
                return outputRow;
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close()
        {
            CursorLifecycle.checkIdleOrActive(this);
            if (!closed) {
                closeOuter();
                closed = true;
            }
        }

        @Override
        public void destroy()
        {
            close();
            outerInput.destroy();
            innerRows.destroy();
        }

        @Override
        public boolean isIdle()
        {
            return closed;
        }

        @Override
        public boolean isActive()
        {
            return !closed;
        }

        @Override
        public boolean isDestroyed()
        {
            return outerInput.isDestroyed();
        }

        // Execution interface

        Execution(QueryContext context)
        {
            super(context);
            this.outerInput = outerInputOperator.cursor(context);
            this.innerRows = new InnerRows(innerInputOperator.cursor(context));
        }

        // For use by this class

        private Row nextProductRow()
        {
            Row productRow = null;
            if (outerRow.isHolding()) {
                Row innerRow = innerRows.next();
                if (innerRow == null) {
                    closeInner();
                } else {
                    productRow = new ProductRow(productType, outerRow.get(), innerRow);
                }
            }
            return productRow;
        }

        private void closeOuter()
        {
            closeInner();
            outerInput.close();
        }

        private void closeInner()
        {
            outerRow.release();
            innerRows.close();
        }

        // Object state

        private final Cursor outerInput;
        private final ShareHolder<Row> outerRow = new ShareHolder<Row>();
        private final ShareHolder<Row> outerBranchRow = new ShareHolder<Row>();
        private final InnerRows innerRows;
        private boolean closed = true;

        // Inner classes

        private class InnerRows
        {
            public InnerRows(Cursor innerInput)
            {
                this.innerInput = innerInput;
            }

            public Row next()
            {
                return scan.next();
            }

            public void resetForCurrentBranchRow()
            {
                scan.reset();
            }

            public void newBranchRow(Row branchRow)
            {
                close();
                context.setRow(inputBindingPosition, branchRow);
                innerInput.open();
                rows.clear();
                Row row;
                while ((row = innerInput.next()) != null) {
                    rows.add(row);
                }
                resetForCurrentBranchRow();
            }

            public void close()
            {
                innerInput.close();
            }

            public void destroy()
            {
                close();
                innerInput.destroy();
            }

            private final Cursor innerInput;
            private final RowList rows = new RowList();
            private final RowList.Scan scan = rows.scan();
        }
    }
}
