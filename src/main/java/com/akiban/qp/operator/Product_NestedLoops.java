/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.qp.operator;

import com.akiban.qp.row.ProductRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.ProductRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.std.NestedLoopsExplainer;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        return new NestedLoopsExplainer(getName(), innerInputOperator, outerInputOperator, innerType, outerType, context);
    }
    
    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, bindingsCursor);
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
        List<Operator> result = new ArrayList<>(2);
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

    private class Execution extends OperatorCursor
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
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                if (CURSOR_LIFECYCLE_ENABLED) {
                    CursorLifecycle.checkIdleOrActive(this);
                }
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
                                    if (LOG_EXECUTION) {
                                        LOG.debug("Product_NestedLoops: branch row {}", row);
                                    }
                                    outerBranchRow.hold(branchRow);
                                    innerRows.newBranchRow(branchRow);
                                }
                                outerRow.hold(row);
                                if (LOG_EXECUTION) {
                                    LOG.debug("Product_NestedLoops: restart inner loop using current branch row");
                                }
                                innerRows.resetForCurrentBranchRow();
                            }
                        }
                    }
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Product_NestedLoops: yield {}", outputRow);
                }
                return outputRow;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
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

        @Override
        public void openBindings() {
            outerInput.openBindings();
        }

        @Override
        public QueryBindings nextBindings() {
            outerBindings = outerInput.nextBindings();
            return outerBindings;
        }

        @Override
        public void closeBindings() {
            outerInput.closeBindings();
        }

        // Execution interface

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor)
        {
            super(context);
            this.outerInput = outerInputOperator.cursor(context, bindingsCursor);
            this.innerRows = new InnerRows(context, bindingsCursor);
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
        private final ShareHolder<Row> outerRow = new ShareHolder<>();
        private final ShareHolder<Row> outerBranchRow = new ShareHolder<>();
        private final InnerRows innerRows;
        private boolean closed = true;
        private QueryBindings outerBindings;

        // Inner classes

        private class InnerRows
        {
            public InnerRows(QueryContext context, QueryBindingsCursor bindingsCursor)
            {
                this.innerBindingsCursor = new SingletonQueryBindingsCursor(null);
                this.innerInput = innerInputOperator.cursor(context, innerBindingsCursor);
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
                outerBindings.setRow(inputBindingPosition, branchRow);
                innerBindingsCursor.reset(outerBindings);
                innerInput.openTopLevel();
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
            private final SingletonQueryBindingsCursor innerBindingsCursor;
        }
    }
}
