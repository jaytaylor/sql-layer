/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.qp.operator;

import com.akiban.qp.exec.Plannable;
import com.akiban.qp.row.ProjectedRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.explain.*;
import com.akiban.server.explain.std.SortOperatorExplainer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.server.types3.texpressions.TEvaluatableExpression;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.WrappingByteSource;
import com.akiban.util.tap.InOutTap;

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
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context, inputOperator.cursor(context));
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
        ArgumentValidation.isGT("limit", limit, 0);
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

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                input.open();
                state = State.FILLING;
                if (usingPValues()) {
                    for (TEvaluatableExpression eval : tEvaluations)
                        eval.with(context);
                }
                else {
                    for (ExpressionEvaluation eval : oEvaluations)
                        eval.of(context);
                }
                sorted = new TreeSet<Holder>();
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            TAP_NEXT.in();
            try {
                CursorLifecycle.checkIdleOrActive(this);
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
                            if (usingPValues())
                                holder = new Holder(label, row, tEvaluations, null);
                            else
                                holder = new Holder(label, row, oEvaluations);
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
                    if (iterator.hasNext()) {
                        Holder holder = iterator.next();
                        return holder.empty();
                    }
                    else {
                        close();
                        return null;
                    }
                case CLOSED:
                    return null;
                case DESTROYED:
                    assert false;
                    return null;
                default:
                    return null;
                }
            } finally {
                TAP_NEXT.out();
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
            if (usingPValues()) {
                // TODO nothing to destroy yet...
            }
            else {
                for (ExpressionEvaluation evaluation : oEvaluations) {
                    evaluation.destroy();
                }
            }
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
        
        // private methods
        
        private boolean usingPValues() {
            return tEvaluations != null;
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context);
            this.input = input;
            int nsort = ordering.sortColumns();
            if (ordering.usingPVals()) {
                tEvaluations = new ArrayList<TEvaluatableExpression>(nsort);
                for (int i = 0; i < nsort; ++i) {
                    TEvaluatableExpression evaluation = ordering.tExpression(i).build();
                    tEvaluations.add(evaluation);
                }
                oEvaluations = null;
            }
            else {
                tEvaluations = null;
                oEvaluations = new ArrayList<ExpressionEvaluation>(nsort);
                for (int i = 0; i < nsort; i++) {
                    ExpressionEvaluation evaluation = ordering.expression(i).evaluation();
                    oEvaluations.add(evaluation);
                }
            }
        }

        // Object state

        private final Cursor input;
        private final List<TEvaluatableExpression> tEvaluations;
        private final List<ExpressionEvaluation> oEvaluations;
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
        private ShareHolder<Row> row;
        private Comparable<Holder>[] values;

        public Holder(int index, Row arow, List<TEvaluatableExpression> evaluations, Void usingPValues) {
            this.index = index;

            row = new ShareHolder<Row>();
            row.hold(arow);

            values = new Comparable[ordering.sortColumns()];
            for (int i = 0; i < values.length; i++) {
                TEvaluatableExpression evaluation = evaluations.get(i);
                evaluation.with(arow);
                evaluation.evaluate();
                values[i] = toObject(evaluation.resultValue());
            }
        }

        public Holder(int index, Row arow, List<ExpressionEvaluation> evaluations) {
            this.index = index;

            row = new ShareHolder<Row>();
            row.hold(arow);

            ToObjectValueTarget target = new ToObjectValueTarget();

            values = new Comparable[ordering.sortColumns()];
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

        // Make sure the Row we save doesn't depend on bindings that may change.
        public void freeze() {
            Row arow = row.get();
            if (arow instanceof ProjectedRow)
                ((ProjectedRow)arow).freeze();
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

        private Comparable toObject(PValueSource valueSource) {
            if (valueSource.isNull())
                return null;
            switch (valueSource.getUnderlyingType()) {
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
                throw new AssertionError(valueSource.getUnderlyingType());
            }
        }
    }
}
