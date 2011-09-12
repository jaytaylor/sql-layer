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

import com.akiban.qp.expression.Expression;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.row.ValuesRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.util.ArgumentValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class Map_NestedLoops extends PhysicalOperator
{
    // Object interface

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    // PhysicalOperator interface

    @Override
    protected Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter);
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        innerInputOperator.findDerivedTypes(derivedTypes);
        outerInputOperator.findDerivedTypes(derivedTypes);
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
        return String.format("%s\n%s", describePlan(outerInputOperator), describePlan(innerInputOperator));
    }

    // Project_Default interface

    public Map_NestedLoops(PhysicalOperator outerInputOperator,
                           PhysicalOperator innerInputOperator,
                           RowType outerJoinRowType,
                           List<Expression> outerJoinRowExpressions,
                           int inputBindingPosition)
    {
        ArgumentValidation.notNull("outerInputOperator", outerInputOperator);
        ArgumentValidation.notNull("innerInputOperator", innerInputOperator);
        ArgumentValidation.isTrue("outer join specification makes sense",
                                  (outerJoinRowType == null && outerJoinRowExpressions == null) ||
                                  (outerJoinRowType != null &&
                                   outerJoinRowExpressions != null &&
                                   outerJoinRowExpressions.size() > 0));
        ArgumentValidation.isGTE("inputBindingPosition", inputBindingPosition, 0);
        this.outerInputOperator = outerInputOperator;
        this.innerInputOperator = innerInputOperator;
        this.outerJoinRowType = outerJoinRowType;
        this.outerJoinRowExpressions = new ArrayList<Expression>(outerJoinRowExpressions);
        this.inputBindingPosition = inputBindingPosition;
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(BranchLookup_Nested.class);

    // Object state

    private final PhysicalOperator outerInputOperator;
    private final PhysicalOperator innerInputOperator;
    private final RowType outerJoinRowType;
    private final List<Expression> outerJoinRowExpressions;
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
                outputRow = nextOutputRow();
                if (outputRow == null) {
                    Row row = outerInput.next();
                    if (row == null) {
                        close();
                    } else {
                        outerRow.set(row);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Map_NestedLoops: restart inner loop using current branch row");
                        }
                        startNewInnerLoop(row);
                    }
                }
            }
            if(LOG.isDebugEnabled()) {
                LOG.debug("Map_NestedLoops: yield {}", outputRow);
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
            this.innerInput = innerInputOperator.cursor(adapter);
        }

        // For use by this class

        private Row nextOutputRow()
        {
            Row outputRow = null;
            if (outerRow.isNotNull()) {
                Row innerRow = innerInput.next();
                if (innerRow == null) {
                    if (needOuterJoinRow) {
                        int nFields = outerJoinRowType.nFields();
                        Object[] fields = new Object[nFields];
                        for (int i = 0; i < nFields; i++) {
                            fields[i] = outerJoinRowExpressions.get(i).evaluate(outerRow.get(), bindings);
                        }
                        outputRow = new ValuesRow(outerJoinRowType, fields);
                        // We're about to emit an outerjoin row. Don't do it again.
                        needOuterJoinRow = false;
                    } else {
                        outerRow.set(null);
                    }
                } else {
                    outputRow = innerRow;
                    needOuterJoinRow = false;
                }
            }
            return outputRow;
        }

        private void closeOuter()
        {
            outerRow.set(null);
            outerInput.close();
        }

        private void startNewInnerLoop(Row row)
        {
            innerInput.close();
            bindings.set(inputBindingPosition, row);
            innerInput.open(bindings);
            needOuterJoinRow = true;
        }

        // Object state

        private final Cursor outerInput;
        private final Cursor innerInput;
        private final RowHolder<Row> outerRow = new RowHolder<Row>();
        private Bindings bindings;
        private boolean closed = false;
        private boolean needOuterJoinRow;
    }
}
