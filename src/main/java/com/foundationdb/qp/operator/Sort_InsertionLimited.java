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

import com.foundationdb.qp.row.ImmutableRow;
import com.foundationdb.qp.row.ProjectedRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.explain.std.SortOperatorExplainer;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.WrappingByteSource;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 <h1>Overview</h1>

 Sort_InsertionLimited provides the first N rows of an input stream after sorting. It is a particularly efficient
 form of sort because it can track the N rows in memory (unless N is too large).

 <h1>Arguments</h1>

 <li><b>Operator inputOperator:</b> Operator providing the input stream.
 <li><b>RowType sortType:</b> Type of rows to be sorted.
 <li><b>API.Ordering ordering:</b> Specification of ordering, comprising a list of expressions and ascending/descending
 specifications.
 <li><b>API.SortOption sortOption:</b> Specifies whether duplicates should be kept (PRESERVE_DUPLICATES) or eliminated
 (SUPPRESS_DUPLICATES)
 <li><b>int limit:</b> Number of rows to keep.

 <h1>Behavior</h1>

 All input rows are examined, and the top limit of them are kept. These rows are emitted in order after the input
 stream has been consumed.

 <h1>Output</h1>

 The first limit rows, according to the ordering specification. The output rows may containg duplicates if and only
 if PRESERVE_DUPLICATE behavior was selected.

 <h1>Assumptions</h1>

 All input rows are of type sortType.

 The limit can be any value, but the sort is done in memory, so bad things (like swapping) will occur if the limit
 is "too large".

 <h1>Performance</h1>

 Sort_InsertionLimited does no IO. For each row, a sorted set of rows is maintained, requiring O(log(limit)) comparisons
 per row.

 <h1>Memory Requirements</h1>

 Up to limit rows are kept in memory.

 */

class Sort_InsertionLimited extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s, %d%s)", getClass().getSimpleName(), sortType, limit,
                             preserveDuplicates ? "" : ", SUPPRESS_DUPLICATES");
    }

    // Operator interface

    @Override
    public List<Operator> getInputOperators()
    {
        return Collections.singletonList(inputOperator);
    }

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, inputOperator.cursor(context, bindingsCursor));
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
                                 API.SortOption sortOption,
                                 int limit)
    {
        ArgumentValidation.notNull("sortType", sortType);
        ArgumentValidation.isGT("ordering.columns()", ordering.sortColumns(), 0);
        ArgumentValidation.isGTE("limit", limit, 0);
        this.inputOperator = inputOperator;
        this.sortType = sortType;
        this.ordering = ordering;
        this.preserveDuplicates = sortOption == API.SortOption.PRESERVE_DUPLICATES;
        this.sortOption = sortOption;
        this.limit = limit;
    }

    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Sort_InsertionLimited open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Sort_InsertionLimited next");
    private static final Logger LOG = LoggerFactory.getLogger(Sort_InsertionLimited.class);

    // Object state
    private final API.SortOption sortOption;
    private final Operator inputOperator;
    private final RowType sortType;
    private final API.Ordering ordering;
    private final boolean preserveDuplicates;
    private final int limit;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new SortOperatorExplainer(getName(), sortOption, sortType, inputOperator, ordering, context);
        ex.addAttribute(Label.LIMIT, PrimitiveExplainer.getInstance(limit));
        return ex;
    }

    // Inner classes

    private enum State { CLOSED, FILLING, EMPTYING, DESTROYED }

    private class Execution extends ChainedCursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                if(limit == 0) {
                    LOG.debug("Sort_InsertionLimited: limit 0, closing");
                    close();
                } else {
                    input.open();
                    state = State.FILLING;
                    for (TEvaluatableExpression eval : tEvaluations) {
                        eval.with(context);
                        eval.with(bindings);
                    }
                    sorted = new TreeSet<>();
                }
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
                switch (state) {
                case FILLING:
                    {
                        // If duplicates are preserved, the label is different for each row. Otherwise, it stays at 0.
                        int label = 0;
                        Row row;
                        while ((row = input.next()) != null) {
                            assert row.rowType() == sortType : row;
                            Holder holder;
                            holder = new Holder(label, row, tEvaluations);
                            if (preserveDuplicates) {
                                label++;
                            }
                            if (sorted.size() < limit) {
                                // Still room: add it in.
                                holder.freeze();
                                boolean added = sorted.add(holder);
                                assert !preserveDuplicates || added;
                            }
                            else {
                                // Current greatest element.
                                Holder last = sorted.last();
                                if (last.compareTo(holder) > 0) {
                                    // New row is less, so keep it
                                    // instead unless it's already in
                                    // there (in suppress dups case).
                                    boolean added = sorted.add(holder);
                                    if (added) {
                                        sorted.remove(last);
                                        last.empty();
                                        holder.freeze();
                                    }
                                    else {
                                        assert !preserveDuplicates;
                                    }
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
                    Row output;
                    if (iterator.hasNext()) {
                        Holder holder = iterator.next();
                        output = holder.empty();
                    }
                    else {
                        close();
                        output = null;
                    }
                    if (LOG_EXECUTION) {
                        LOG.debug("Sort_InsertionLimited: yield {}", output);
                    }
                    return output;
                case DESTROYED:
                    assert false;
                    // Fall through
                case CLOSED:
                default:
                    if (LOG_EXECUTION) {
                        LOG.debug("Sort_InsertionLimited: yield null");
                    }
                    return null;
                }
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

        @Override
        public void destroy()
        {
            close();
            input.destroy();
            // TODO nothing to destroy for expressions yet
            state = State.DESTROYED;
        }

        @Override
        public boolean isIdle()
        {
            return state == State.CLOSED;
        }

        @Override
        public boolean isActive()
        {
            return state == State.FILLING || state == State.EMPTYING;
        }

        @Override
        public boolean isDestroyed()
        {
            return state == State.DESTROYED;
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context, input);
            int nsort = ordering.sortColumns();
            tEvaluations = new ArrayList<>(nsort);
            for (int i = 0; i < nsort; ++i) {
                TEvaluatableExpression evaluation = ordering.expression(i).build();
                tEvaluations.add(evaluation);
            }
        }

        // Object state

        private final List<TEvaluatableExpression> tEvaluations;
        private State state = State.CLOSED;
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
        private Row row;
        private Comparable[] values;

        public Holder(int index, Row arow, List<TEvaluatableExpression> evaluations) {
            this.index = index;
            this.row = arow;

            values = new Comparable[ordering.sortColumns()];
            for (int i = 0; i < values.length; i++) {
                TEvaluatableExpression evaluation = evaluations.get(i);
                evaluation.with(arow);
                evaluation.evaluate();
                values[i] = toObject(evaluation.resultValue());
            }
        }

        public Row empty() {
            Row result = row;
            row = null;
            return result;
        }

        // Make sure the Row we save doesn't depend on bindings that may change.
        public void freeze() {
            if (row.isBindingsSensitive()) {
                row = ImmutableRow.buildImmutableRow(row);
            }
        }

        @SuppressWarnings("unchecked")
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
                int comp;
                if (ordering.collator(i) == null) {
                    comp = v1.compareTo(v2);
                }
                else {
                    comp = ordering.collator(i).compare(v1.toString(), v2.toString());
                }
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

        private Comparable toObject(ValueSource valueSource) {
            if (valueSource.isNull())
                return null;
            switch (ValueSources.underlyingType(valueSource)) {
            case BOOL:
                return valueSource.getBoolean();
            case INT_8:
                return valueSource.getInt8();
            case INT_16:
                return valueSource.getInt16();
            case UINT_16:
                return valueSource.getUInt16();
            case INT_32:
                return valueSource.getInt32();
            case INT_64:
                return valueSource.getInt64();
            case FLOAT:
                return valueSource.getFloat();
            case DOUBLE:
                return valueSource.getDouble();
            case BYTES:
                return new WrappingByteSource(valueSource.getBytes());
            case STRING:
                return valueSource.getString();
            default:
                throw new AssertionError(valueSource.getType());
            }
        }
    }
}
