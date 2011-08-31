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
import com.akiban.qp.rowtype.RowType;
import com.akiban.util.ArgumentValidation;

import java.util.*;

class Sort_InsertionLimited extends PhysicalOperator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s, %d)", getClass().getSimpleName(), sortType, limit);
    }

    // PhysicalOperator interface

    @Override
    public List<PhysicalOperator> getInputOperators()
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

    public Sort_InsertionLimited(PhysicalOperator inputOperator, 
                                 RowType sortType,
                                 List<Expression> sortExpressions,
                                 List<Boolean> sortDescendings,
                                 int limit)
    {
        ArgumentValidation.notNull("sortType", sortType);
        ArgumentValidation.notEmpty("sortExpressions", sortExpressions);
        ArgumentValidation.notNull("sortDescendings", sortDescendings);
        ArgumentValidation.isEQ("sortExpressions.size()", sortExpressions.size(),
                                "sortDescendings.size()", sortDescendings.size());
        ArgumentValidation.isGT("limit", limit, 0);
        this.inputOperator = inputOperator;
        this.sortType = sortType;
        this.sortExpressions = sortExpressions;
        this.sortDescendings = sortDescendings;
        this.limit = limit;
    }

    // Object state

    private final PhysicalOperator inputOperator;
    private final RowType sortType;
    private final List<Expression> sortExpressions;
    private final List<Boolean> sortDescendings;
    private final int limit;

    // Inner classes

    private enum State { CLOSED, FILLING, EMPTYING }

    private class Execution implements Cursor
    {
        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            input.open(bindings);
            state = State.FILLING;
            this.bindings = bindings;
            sorted = new TreeSet<Holder>();
        }

        @Override
        public Row next()
        {
            switch (state) {
            case FILLING:
                {
                    int count = 0;
                    Row row;
                    while ((row = input.next()) != null) {
                        assert row.rowType() == sortType : row;
                        Holder holder = new Holder(count++, row, bindings);
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
            this.input = input;
        }

        // Object state

        private final Cursor input;
        private State state;
        private Bindings bindings;
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
        private RowHolder row;
        private Comparable[] values;

        public Holder(int index, Row arow, Bindings bindings) {
            this.index = index;

            row = new RowHolder();
            row.set(arow);

            values = new Comparable[sortExpressions.size()];
            for (int i = 0; i < values.length; i++) {
                values[i] = (Comparable)sortExpressions.get(i).evaluate(arow, bindings);
            }
        }

        public Row empty() {
            Row result = row.get();
            row.set(null);
            return result;
        }

        public int compareTo(Holder other) {
            for (int i = 0; i < values.length; i++) {
                Comparable v1 = values[i];
                Comparable v2 = other.values[i];
                int less, greater;
                if (sortDescendings.get(i).booleanValue()) {
                    less = +1;
                    greater = -1;
                }
                else {
                    less = -1;
                    greater = +1;
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
