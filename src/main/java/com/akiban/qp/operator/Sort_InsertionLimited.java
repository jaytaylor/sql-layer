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

package com.akiban.qp.operator;

import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;

import java.util.*;

class Sort_InsertionLimited extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s, %d)", getClass().getSimpleName(), sortType, limit);
    }

    // Operator interface

    @Override
    public List<Operator> getInputOperators()
    {
        return Collections.singletonList(inputOperator);
    }

    @Override
    protected Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter, inputOperator.cursor(adapter));
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

    // Sort_InsertionLimited interface

    public Sort_InsertionLimited(Operator inputOperator,
                                 RowType sortType,
                                 API.Ordering ordering,
                                 int limit)
    {
        ArgumentValidation.notNull("sortType", sortType);
        ArgumentValidation.isGT("ordering.columns()", ordering.sortFields(), 0);
        ArgumentValidation.isGT("limit", limit, 0);
        this.inputOperator = inputOperator;
        this.sortType = sortType;
        this.ordering = ordering;
        this.limit = limit;
    }

    // Object state

    private final Operator inputOperator;
    private final RowType sortType;
    private final API.Ordering ordering;
    private final int limit;

    // Inner classes

    private enum State { CLOSED, FILLING, EMPTYING }

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            input.open(bindings);
            state = State.FILLING;
            for (ExpressionEvaluation eval : evaluations)
                eval.of(bindings);
            sorted = new TreeSet<Holder>();
        }

        @Override
        public Row next()
        {
            checkQueryCancelation();
            switch (state) {
            case FILLING:
                {
                    int count = 0;
                    Row row;
                    while ((row = input.next()) != null) {
                        assert row.rowType() == sortType : row;
                        Holder holder = new Holder(count++, row, evaluations);
                        if (sorted.size() < limit) {
                            // Still room: add it in.
                            boolean added = sorted.add(holder);
                            assert added;
                        }
                        else {
                            // Current greatest element.
                            Holder last = sorted.last();
                            if (last.compareTo(holder) > 0) {
                                // New row is less, so keep it
                                // instead.
                                sorted.remove(last);
                                last.empty();
                                boolean added = sorted.add(holder);
                                assert added;
                            }
                            else {
                                // Will not be using new row.
                                holder.empty();
                            }
                        }
                    }
                    iterator = sorted.iterator();
                    state = State.EMPTYING;
                }
                /* falls through */
            case EMPTYING:
                if (iterator.hasNext()) {
                    Holder holder = iterator.next();
                    return holder.empty();
                }
                else {
                    close();
                    return null;
                }
            case CLOSED:
            default:
                return null;
            }
        }

        @Override
        public void close()
        {
            input.close();
            if (sorted != null) {
                if (iterator == null)
                    iterator = sorted.iterator();
                while (iterator.hasNext()) {
                    iterator.next().empty();
                }
                iterator = null;
                sorted = null;
            }
            state = State.CLOSED;
        }

        // Execution interface

        Execution(StoreAdapter adapter, Cursor input)
        {
            super(adapter);
            this.input = input;
            int nsort = ordering.sortFields();
            evaluations = new ArrayList<ExpressionEvaluation>(nsort);
            for (int i = 0; i < nsort; i++) {
                ExpressionEvaluation evaluation = ordering.expression(i).evaluation();
                evaluation.of(adapter);
                evaluations.add(evaluation);
            }
        }

        // Object state

        private final Cursor input;
        private final List<ExpressionEvaluation> evaluations;
        private State state;
        private SortedSet<Holder> sorted;
        private Iterator<Holder> iterator;
    }

    // Sortable row holder.

    // Since a SortedSet cannot have two elements that compare as 0,
    // we never return that, instead ordering things that sort the
    // same based on their arrival order. For the same reason, we do
    // not need to overload equals().
    private class Holder implements Comparable<Holder> {
        private int index;
        private ShareHolder<Row> row;
        private ToObjectValueTarget target = new ToObjectValueTarget();
        private Comparable[] values;

        public Holder(int index, Row arow, List<ExpressionEvaluation> evaluations) {
            this.index = index;

            row = new ShareHolder<Row>();
            row.hold(arow);

            values = new Comparable[ordering.sortFields()];
            for (int i = 0; i < values.length; i++) {
                ExpressionEvaluation evaluation = evaluations.get(i);
                evaluation.of(arow);
                ValueSource valueSource = evaluation.eval();
                target.expectType(ordering.type(i));
                Converters.convert(valueSource, target);
                values[i] = (Comparable) target.lastConvertedValue();
            }
        }

        public Row empty() {
            Row result = row.get();
            row.release();
            return result;
        }

        public int compareTo(Holder other) {
            for (int i = 0; i < values.length; i++) {
                Comparable v1 = values[i];
                Comparable v2 = other.values[i];
                int less, greater;
                if (ordering.ascending(i)) {
                    less = -1;
                    greater = +1;
                }
                else {
                    less = +1;
                    greater = -1;
                }
                if (v1 == null) {
                    if (v2 == null) {
                        continue;
                    }
                    else {
                        return less;
                    }
                }
                else if (v2 == null) {
                    return greater;
                }
                int comp = v1.compareTo(v2);
                if (comp != 0) {
                    if (comp < 0)
                        return less;
                    else
                        return greater;
                }
            }
            return index - other.index;
        }

        public String toString() {
            return row.toString();
        }
    }
}
