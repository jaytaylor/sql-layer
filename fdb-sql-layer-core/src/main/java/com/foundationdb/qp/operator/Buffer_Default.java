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

import com.foundationdb.qp.operator.API.Ordering;
import com.foundationdb.qp.operator.API.SortOption;
import com.foundationdb.qp.row.CompoundRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.BufferRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.Attributes;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.explain.Type;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.texpressions.TPreparedField;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class Buffer_Default extends Operator
{
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Buffer_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Buffer_Default next");
    private static final InOutTap TAP_LOAD = OPERATOR_TAP.createSubsidiaryTap("operator: Buffer_Default load");
    private static final Logger LOG = LoggerFactory.getLogger(Buffer_Default.class);

    private final Operator inputOperator;
    private final BufferRowType bufferRowType;
    private final SortOption sortOption;
    private final Ordering ordering;


    Buffer_Default(Operator inputOperator, RowType inputRowType) {
        ArgumentValidation.notNull("inputOperator", inputOperator);
        ArgumentValidation.notNull("inputRowType", inputRowType);
        this.inputOperator = inputOperator;
        this.bufferRowType = inputRowType.schema().bufferRowType(inputRowType);
        this.sortOption = SortOption.PRESERVE_DUPLICATES; // There shouldn't be any
        this.ordering = API.ordering();
        ordering.append(new TPreparedField(bufferRowType.first().typeAt(0), 0), true);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getClass().getSimpleName());
        str.append("(");
        str.append(inputOperator);
        str.append(")");
        return str.toString();
    }


    //
    // Operator
    //

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        return new Execution(context, inputOperator.cursor(context, bindingsCursor));
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes) {
        inputOperator.findDerivedTypes(derivedTypes);
    }


    //
    // Plannable
    //

    @Override
    public List<Operator> getInputOperators() {
        return Collections.singletonList(inputOperator);
    }

    @Override
    public String describePlan() {
        return super.describePlan();
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.INPUT_OPERATOR, inputOperator.getExplainer(context));
        return new CompoundExplainer(Type.BUFFER_OPERATOR, atts);
    }


    //
    // Internal
    //

    private class BufferRowCreatorCursor extends ChainedCursor
    {
        private long counter;

        public BufferRowCreatorCursor(QueryContext context, Cursor input) {
            super(context, input);
        }

        @Override
        public Row next() {
            Row inputRow = input.next();
            if(inputRow != null) {
                ValuesHolderRow counterRow = new ValuesHolderRow(bufferRowType.first());
                counterRow.valueAt(0).putInt64(counter++);
                inputRow = new CompoundRow(bufferRowType, counterRow, inputRow);
            }
            return inputRow;
        }
    }

    private class Execution extends ChainedCursor {
        private SorterToCursorAdapter sorter;
        private BufferRowCreatorCursor creatorCursor;

        public Execution(QueryContext context, Cursor input) {
            super(context, input);
        }

        // Cursor interface

        @Override
        public void open() {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkClosed(this);
                // Eager load
                creatorCursor = new BufferRowCreatorCursor(context, input);
                creatorCursor.open(); // opens the input cursor too.
                state = CursorLifecycle.CursorState.ACTIVE;
                sorter = new SorterToCursorAdapter(adapter(), context, bindings, creatorCursor, bufferRowType, ordering, sortOption, TAP_LOAD);
                sorter.open();
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next() {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                if (CURSOR_LIFECYCLE_ENABLED) {
                    CursorLifecycle.checkIdleOrActive(this);
                }
                Row next = sorter.next();
                if(next != null) {
                    assert next.rowType() == bufferRowType;
                    // Common case coming out of default Sorters
                    if(next instanceof ValuesHolderRow) {
                        ValuesHolderRow valuesRow = (ValuesHolderRow)next;
                        RowType realRowType = bufferRowType.second();
                        List<Value> values = valuesRow.values();
                        next = new ValuesHolderRow(realRowType, values.subList(1, realRowType.nFields() + 1));
                    } else if(next instanceof CompoundRow) {
                        next = ((CompoundRow)next).subRow(bufferRowType.second());
                    } else {
                        throw new IllegalStateException("Unexpected Row: " + next.getClass());
                    }
                }
                return next;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        @Override
        public void close() {
            super.close();
            sorter.close();
            if (creatorCursor != null) {
                creatorCursor.close();
            }
        }
    }
}
