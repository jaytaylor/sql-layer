/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.qp.physicaloperator;

import com.akiban.qp.row.ProductRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.ProductRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.util.ArgumentValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class Product_NestedLoops extends PhysicalOperator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s x %s)", getClass().getSimpleName(), outerType, innerType);
    }

    // PhysicalOperator interface

    @Override
    protected Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter);
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
    public List<PhysicalOperator> getInputOperators()
    {
        List<PhysicalOperator> result = new ArrayList<PhysicalOperator>(2);
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

    public Product_NestedLoops(PhysicalOperator outerInputOperator,
                               PhysicalOperator innerInputOperator,
                               RowType outerType,
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
        this.productType = outerType.schema().newProductType(outerType, innerType);
        this.branchType = productType.branchType();
        this.inputBindingPosition = inputBindingPosition;
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(BranchLookup_Nested.class);

    // Object state

    private final PhysicalOperator outerInputOperator;
    private final PhysicalOperator innerInputOperator;
    private final RowType branchType;
    private final RowType outerType;
    private final RowType innerType;
    private final ProductRowType productType;
    private final int inputBindingPosition;

    // Inner classes

    private class Execution implements Cursor
    {
        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            this.bindings = bindings;
            this.outerInput.open(bindings);
        }

        @Override
        public Row next()
        {
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
                            if (outerBranchRow.isNull() || !branchRow.hKey().equals(outerBranchRow.get().hKey())) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Product_NestedLoops: branch row {}", row);
                                }
                                outerBranchRow.set(branchRow);
                                innerRows.newBranchRow(branchRow);
                            }
                            outerRow.set(row);
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
        }

        @Override
        public void close()
        {
            if (!closed) {
                closeOuter();
                closed = true;
            }
        }

        // Execution interface

        Execution(StoreAdapter adapter)
        {
            this.outerInput = outerInputOperator.cursor(adapter);
            this.innerRows = new InnerRows(innerInputOperator.cursor(adapter));
        }

        // For use by this class

        private Row nextProductRow()
        {
            Row productRow = null;
            if (outerRow.isNotNull()) {
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
            outerRow.set(null);
            innerRows.close();
        }

        // Object state

        private final Cursor outerInput;
        private final RowHolder<Row> outerRow = new RowHolder<Row>();
        private final RowHolder<Row> outerBranchRow = new RowHolder<Row>();
        private final InnerRows innerRows;
        private Bindings bindings;
        private boolean closed = false;

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
                bindings.set(inputBindingPosition, branchRow);
                innerInput.open(bindings);
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

            private final Cursor innerInput;
            private final RowList rows = new RowList();
            private final RowList.Scan scan = rows.scan();
        }
    }
}
