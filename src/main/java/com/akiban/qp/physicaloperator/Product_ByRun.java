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
import com.akiban.qp.row.RowBase;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.ProductRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.util.ArgumentValidation;

import java.util.ArrayList;
import java.util.List;

class Product_ByRun extends PhysicalOperator
{
    // Object interface

    @Override
    public String toString()
    {
        return productType.toString();
    }

    // PhysicalOperator interface

    @Override
    protected Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter, inputOperator.cursor(adapter));
    }

    public ProductRowType rowType()
    {
        return productType;
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
        this.productType = leftType.schema().newProductType(leftType, rightType);
    }

    // Object state

    private final PhysicalOperator inputOperator;
    private final RowType leftType;
    private final RowType rightType;
    private final ProductRowType productType;

    // Inner classes

    private class Execution extends SingleRowCachingCursor
    {
        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            input.open(bindings);
        }

        @Override
        public boolean next()
        {
            Row row = null;
            while (row == null && (runState.compareTo(RunState.AFTER_RIGHT) < 0 || input.next())) {
                if (runState == RunState.RIGHT) {
                    row = nextProductRow();
                }
                if (row == null) {
                    if (input.next()) {
                        Row inputRow = input.currentRow();
                        if (inputRow.runId() == RowBase.UNDEFINED_RUN_ID) {
                            throw new IncompatibleRowException
                                ("Product_ByRun cannot take input from a GroupScan_Default");
                        }
                        if (inputRow.runId() != currentRunId) {
                            startNewRun(inputRow);
                        }
                        setRunState(inputRow);
                        switch (runState) {
                            case BEFORE_LEFT:
                                row = inputRow;
                                break;
                            case LEFT:
                                rememberLeftRow(inputRow);
                                break;
                            case BETWEEN:
                                row = inputRow;
                                break;
                            case RIGHT:
                                rememberRightRow(inputRow);
                                break;
                            case AFTER_RIGHT:
                                row = inputRow;
                                terminateRunProduct();
                                break;
                        }
                    } else {
                        runState = RunState.AFTER_RIGHT;

                    }
                }
            }
            outputRow(row);
            return row != null;
        }

        @Override
        public void close()
        {
            outputRow(null);
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
                case BETWEEN:
                    if (rowType == rightType) {
                        runState = RunState.RIGHT;
                    }
                    break;
                case RIGHT:
                    if (rowType != rightType) {
                        runState = RunState.AFTER_RIGHT;
                    }
                    break;
                case AFTER_RIGHT:
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
        private int currentRunId = RowBase.UNDEFINED_RUN_ID;
        private RunState runState = RunState.BEFORE_LEFT;
    }

    private enum RunState
    {
        BEFORE_LEFT, LEFT, BETWEEN, RIGHT, AFTER_RIGHT
    }
}
