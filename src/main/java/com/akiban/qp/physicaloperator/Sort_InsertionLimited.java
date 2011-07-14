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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

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

    private enum State { CLOSED, FILLING, EMPTYING };

    private class Execution implements Cursor
    {
        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            input.open(bindings);
            state = State.FILLING;
            this.bindings = bindings;
            queue = new PriorityQueue<Holder>();
        }

        @Override
        public Row next()
        {
            switch (state) {
            case FILLING:
                {
                    Row row;
                    while ((row = input.next()) != null) {
                        if (row.rowType() == sortType) {
                            Holder holder = new Holder(row, bindings);
                            if (queue.size() < limit) {
                                // Still room: add it in.
                                queue.add(holder);
                            }
                            else {
                                // The least element according to Holder's
                                // ordering, which would be the last in
                                // the sort order.
                                Holder last = queue.peek();
                                if (last.compareTo(holder) < 0) {
                                    // New row is less (later in sort
                                    // order), so keep it instead.
                                    last = queue.poll();
                                    last.empty();
                                    queue.add(holder);
                                }
                                else {
                                    // Will not be using new row.
                                    holder.empty();
                                }
                            }
                        }
                    }
                    count = queue.size();
                    sorted = queue.toArray(new Holder[count]);
                    queue = null;
                    Arrays.sort(sorted);
                    state = State.EMPTYING;
                }
                /* falls through */
            case EMPTYING:
                if (count > 0) {
                    count--;
                    Holder holder = sorted[count];
                    sorted[count] = null;
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
            switch (state) {
            case FILLING:
                {
                    Holder holder;
                    while ((holder = queue.poll()) != null) {
                        holder.empty();
                    }
                    queue = null;
                }
                break;
            case EMPTYING:
                for (int i = 0; i < count; i++) {
                    sorted[i].empty();
                }
                sorted = null;
                break;
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
        private PriorityQueue<Holder> queue;
        private Holder[] sorted;
        private int count;
    }

    // Sortable row holder.

    // In order to keep the first n according to the sort order, we
    // need the priority queue to give up the greatest so far, so the
    // holder's ordering is the reverse of the sort's.
    private class Holder implements Comparable<Holder> {
        private RowHolder row;
        private Comparable[] values;

        public Holder(Row arow, Bindings bindings) {
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
            return 0;
        }

        public String toString() {
            return row.toString();
        }
    }
}
