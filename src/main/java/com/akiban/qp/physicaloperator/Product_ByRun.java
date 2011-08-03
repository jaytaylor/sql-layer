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

import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.ProductRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.ProductRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.util.ArgumentValidation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class Product_ByRun extends PhysicalOperator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s x %s)", getClass().getSimpleName(), leftType, rightType);
    }

    // PhysicalOperator interface

    @Override
    protected Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter, inputOperator.cursor(adapter));
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
    public List<PhysicalOperator> getInputOperators()
    {
        List<PhysicalOperator> result = new ArrayList<PhysicalOperator>(1);
        result.add(inputOperator);
        return result;
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Project_Default interface

    public Product_ByRun(PhysicalOperator inputOperator, RowType leftType, RowType rightType)
    {
        ArgumentValidation.notNull("leftChildType", leftType);
        ArgumentValidation.notNull("rightChildType", rightType);
        this.inputOperator = inputOperator;
        this.leftType = leftType;
        this.rightType = rightType;
        // Figure out the branch type. TODO: do this for Product_NestedLoops too?
        Set<UserTable> common = new HashSet<UserTable>(leftType.typeComposition().tables());
        common.retainAll(rightType.typeComposition().tables());
        UserTable leafmostCommon = null;
        for (UserTable table : common) {
            if (leafmostCommon == null || table.getDepth() > leafmostCommon.getDepth()) {
                leafmostCommon = table;
            }
        }
        Schema schema = leftType.schema();
        UserTableRowType branchType = schema.userTableRowType(leafmostCommon);
        this.productType = schema.newProductType(branchType, leftType, rightType);
    }

    // Object state

    private final PhysicalOperator inputOperator;
    private final RowType leftType;
    private final RowType rightType;
    private final ProductRowType productType;

    // Inner classes

    private class Execution implements Cursor
    {
        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            input.open(bindings);
        }

        @Override
        public Row next()
        {
            Row row = null;
            if (runState == RunState.RIGHT) {
                row = nextProductRow();
            }
            while (row == null && (runState.compareTo(RunState.AFTER_RIGHT) < 0 || inputRow.isNotNull())) {
                inputRow.set(input.next());
                Row currentRow = inputRow.get();
                if (currentRow == null) {
                    runState = RunState.AFTER_RIGHT;
                } else {
                    if (currentRow.runId() == RowBase.UNDEFINED_RUN_ID) {
                        throw new IncompatibleRowException
                            ("Product_ByRun cannot take input from a GroupScan_Default");
                    }
                    if (currentRow.runId() != currentRunId) {
                        startNewRun(currentRow);
                    }
                    setRunState(currentRow);
                    switch (runState) {
                        case BEFORE_LEFT:
                            row = currentRow;
                            break;
                        case LEFT:
                            rememberLeftRow(currentRow);
                            break;
                        case BETWEEN:
                            row = currentRow;
                            break;
                        case RIGHT:
                            rememberRightRow(currentRow);
                            row = nextProductRow();
                            break;
                        case AFTER_RIGHT:
                            row = currentRow;
                            terminateRunProduct();
                            break;
                    }
                }
            }
            return row;
        }

        @Override
        public void close()
        {
            input.close();
        }

        // Execution interface

        Execution(StoreAdapter adapter, Cursor input)
        {
            this.input = input;
        }

        // For use by this class

        private Row nextProductRow()
        {
            Row productRow = null;
            if (rightRow.isNotNull()) {
                Row leftRow = leftScan.next();
                if (leftRow != null) {
                    productRow = new ProductRow(productType, leftRow, rightRow.get());
                }
            }
            return productRow;
        }

        private void startNewRun(Row row)
        {
            terminateRunProduct();
            currentRunId = row.runId();
            runState = RunState.BEFORE_LEFT;
        }

        private void setRunState(Row row)
        {
            RowType rowType = row.rowType();
            switch (runState) {
                case BEFORE_LEFT:
                    if (rowType == leftType) {
                        runState = RunState.LEFT;
                    } else if (rowType == rightType) {
                        runState = RunState.RIGHT;
                    }
                    break;
                case LEFT:
                    if (rowType == rightType) {
                        runState = RunState.RIGHT;
                    } else if (rowType != leftType) {
                        runState = RunState.BETWEEN;
                    }
                    break;
                case BETWEEN:
                    if (rowType == leftType) {
                        throw new IncompatibleRowException(String.format("Unexpected appearance of %s", row));
                    } else if (rowType == rightType) {
                        runState = RunState.RIGHT;
                    }
                    break;
                case RIGHT:
                    if (rowType == leftType) {
                        throw new IncompatibleRowException(String.format("Unexpected appearance of %s", row));
                    } else if (rowType != rightType) {
                        runState = RunState.AFTER_RIGHT;
                    }
                    break;
                case AFTER_RIGHT:
                    if (rowType == leftType || rowType == rightType) {
                        throw new IncompatibleRowException(String.format("Unexpected appearance of %s", row));
                    }
                    break;
            }
        }

        private void rememberLeftRow(Row row)
        {
            leftRows.add(row);
        }

        private void rememberRightRow(Row row)
        {
            rightRow.set(row);
            leftScan = leftRows.scan();
        }

        private void terminateRunProduct()
        {
            leftRows.clear();
            rightRow.set(null);
        }

        // Object state

        private final Cursor input;
        private final RowList leftRows = new RowList();
        private RowList.Scan leftScan;
        private final RowHolder<Row> rightRow = new RowHolder<Row>();
        private final RowHolder<Row> inputRow = new RowHolder<Row>();
        private int currentRunId = RowBase.UNDEFINED_RUN_ID;
        private RunState runState = RunState.BEFORE_LEFT;
    }

    private enum RunState
    {
        BEFORE_LEFT, LEFT, BETWEEN, RIGHT, AFTER_RIGHT
    }
}
