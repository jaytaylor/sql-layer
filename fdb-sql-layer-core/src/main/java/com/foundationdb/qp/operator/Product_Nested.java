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

import com.foundationdb.qp.row.ProductRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.ProductRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**

 <h1>Overview</h1>

 Product_Nested computes the cartesian product of child rows owned by the same parent row. For example, if customer c1 has orders o10 and o11, and addresses a10 and a11; and customer c2 has order o20 and address a20, then Product_Nested computes \{o10, o11\} x \{a10, a11\} for c1, and \{o20\} x \{a20\} for c2.

 <h1>Arguments</h1>

 <ul>

 <li><b>RowType outerType:</b> Type of one child row. parent rows to be
 flattened.

 <li><b>TableRowType branchType:</b> Ancestor type of outerType and inputType. Output will consist
 of the cartesian product of outer and input rows that match when projected to the branch type.

 <li><b>RowType inputType:</b> Type of the other child row.

 <li><b>int bindingPosition:</b> The position in the bindings that
 will be used to pass rows from the outer loop to the inner loop.

 </ul>

 The row types must have resulted from Flattens of some common
 ancestor, e.g. Flatten(customer, order) and Flatten(customer,
 address).

 <h1>Behavior</h1>

 Suppose we have a COA schema (C is the parent of O and A), and that we
 have flattened C with O, and C with A. Product_Nested has two
 input streams, with CO in one and CA in the other. The branch type is C.
 For this discussion, let's assume that CO is the outer stream, and CA is in the
 input stream.

 For each CO row from the outer stream, a set of matching CA rows,
 (i.e., matching in C)
 from the input stream are retrieved. The CO row and all of the
 CA rows will have the same customer primary key. Product_Nested
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

 Product_Nested does no IO.

 */

class Product_Nested extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s x %s)", getClass().getSimpleName(), outerType, inputType);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.OUTER_TYPE, outerType.getExplainer(context));
        atts.put(Label.INNER_TYPE, inputType.getExplainer(context));
        atts.put(Label.INPUT_OPERATOR, inputOperator.getExplainer(context));
        atts.put(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(bindingPosition));
        return new CompoundExplainer(Type.PRODUCT_OPERATOR, atts);
    }
    
    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, inputOperator.cursor(context, bindingsCursor));
    }

    @Override
    public ProductRowType rowType()
    {
        return productType;
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
        derivedTypes.add(productType);
    }

    @Override
    public List<Operator> getInputOperators()
    {
        List<Operator> result = new ArrayList<>(2);
        result.add(inputOperator);
        return result;
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Product_Nested interface

    public Product_Nested(Operator inputOperator,
                          RowType outerType,
                          TableRowType branchType,
                          RowType inputType,
                          int bindingPosition)
    {
        ArgumentValidation.notNull("inputOperator", inputOperator);
        ArgumentValidation.notNull("outerType", outerType);
        ArgumentValidation.notNull("inputType", inputType);
        ArgumentValidation.isGTE("bindingPosition", bindingPosition, 0);
        this.inputOperator = inputOperator;
        this.outerType = outerType;
        this.inputType = inputType;
        this.productType = inputType.schema().newProductType(outerType, branchType, inputType);
        this.branchType = productType.branchType();
        this.bindingPosition = bindingPosition;
    }

    // Class state

    private static InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Product_Nested open");
    private static InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Product_Nested next");
    private static final Logger LOG = LoggerFactory.getLogger(Product_Nested.class);

    // Object state

    private final Operator inputOperator;
    private final RowType branchType;
    private final RowType outerType;
    private final RowType inputType;
    private final ProductRowType productType;
    private final int bindingPosition;

    // Inner classes

    private class Execution extends ChainedCursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            super.open();
            Row row = bindings.getRow(bindingPosition);
            assert (row.rowType() == outerType) : row;
            boundRow = row;
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
                Row row = input.next();
                if ((row != null) && (row.rowType() == inputType)) {
                    row = new ProductRow(productType, boundRow, row);
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Product_Nested: yield {}", row);
                }
                return row;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        @Override
        public void close()
        {
            super.close();
            boundRow = null;
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context, input);
        }

        // Object state

        private Row boundRow;
    }
}
