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

import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.BloomFilter;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * <h1>Overview</h1>
 * <p/>
 * Select_BloomFilter checks whether an input row, projected to a given set of expressions, is present in a
 * "filtering" set of rows. The implementation is optimized to avoid IO operations on the filtering set of rows
 * through the use of a bloom filter.
 * <p/>
 * <h1>Arguments</h1>
 * <p/>
 *
 * <li><b>Operator input:</b></li> Operator providing the input stream
 * <li><b>Operator onPositive:</b></li> Operator used to provide output rows
 * <li><b>List<BoundFieldExpression> fields:</b></li> expressions applied to the input row to obtain a
 * bloom filter hash key
 * <li><b>int bindingPosition:</b></li> Location in the query context of the bloom filter, which has been
 * loaded by the Using_BloomFilter operator. This bindingPosition is also used to hold a row from
 * the input stream that passes the filter and needs to be tested for existence using the onPositive
 * operator.
 <li><b>boolean pipeline:</b> Whether to use bracketing cursors instead of rebinding.

 <li><b>int depth:</b> Number of nested Maps, including this one.

 * <p/>
 * <h1>Behavior</h1>
 * <p/>
 * Each call of next operates as follows. A row is obtained from the input operator. The expressions from fields are
 * evaluated and the resulting values
 * are used to probe the bloom filter. If the filter returns false, this indicates that there is no matching row
 * in the filtering set of rows and null is returned. If the filter returns true, then the onPositive operator
 * is used to locate the matching row. If a row is located then the input row (not the row from onPositive)
 * is returned, otherwise null is returned.
 * <p/>
 * <h1>Output</h1>
 * <p/>
 * A subset of rows from the input stream.
 * <p/>
 * <h1>Assumptions</h1>
 * <p/>
 * None.
 * <p/>
 * <h1>Performance</h1>
 * <p/>
 * This operator should generate very little IO activity, although bloom filters are probabilistic.
 * <p/>
 * <h1>Memory Requirements</h1>
 * <p/>
 * This operator relies on the bloom filter created by Using_BloomFilter.
 */

class Select_BloomFilter extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    // Operator interface


    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
        onPositive.findDerivedTypes(derivedTypes);
    }

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        if (!pipeline) {
            return new Execution<>(context, bindingsCursor, tFields, newExpressionsAdapter);
        }
        else {
            assert (tFields != null);
            Cursor inputCursor = inputOperator.cursor(context, bindingsCursor);
            QueryBindingsCursor toBindings = new FilterBindingsCursor(context, inputCursor, bindingPosition, depth, tFields, newExpressionsAdapter);
            Cursor checkCursor = onPositive.cursor(context, toBindings);
            return new RecoverRowsCursor(context, checkCursor, bindingPosition, depth);
        }
    }

    @Override
    public List<Operator> getInputOperators()
    {
        return Arrays.asList(inputOperator, onPositive);
    }

    @Override
    public String describePlan()
    {
        return String.format("%s\n%s", describePlan(inputOperator), describePlan(onPositive));
    }

    // Select_BloomFilter interface

    public Select_BloomFilter(Operator input,
                              Operator onPositive,
                              List<? extends TPreparedExpression> tFields,
                              List<AkCollator> collators,
                              int bindingPosition,
                              boolean pipeline,
                              int depth)
    {
        ArgumentValidation.notNull("input", input);
        ArgumentValidation.notNull("onPositive", onPositive);
        ArgumentValidation.notNull("Fields", tFields);
        int size = tFields.size();
        ArgumentValidation.isGT("fields.size()", size, 0);
        ArgumentValidation.isGTE("bindingPosition", bindingPosition, 0);
        ArgumentValidation.isGT("depth", depth, 0);
        this.inputOperator = input;
        this.onPositive = onPositive;
        this.bindingPosition = bindingPosition;
        this.pipeline = pipeline;
        this.depth = depth;
        this.tFields = tFields;
        this.collators = collators;
    }

    // For use by this class

    private AkCollator collator(int f)
    {
        return collators == null ? null : collators.get(f);
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Select_BloomFilter open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Select_BloomFilter next");
    private static final InOutTap TAP_CHECK = OPERATOR_TAP.createSubsidiaryTap("operator: Select_BloomFilter check");
    private static final Logger LOG = LoggerFactory.getLogger(Select_BloomFilter.class);

    // Object state

    private final Operator inputOperator;
    private final Operator onPositive;
    private final int bindingPosition, depth;
    private final boolean pipeline;
    private final List<? extends TPreparedExpression> tFields;
    private final List<AkCollator> collators;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(bindingPosition));
        atts.put(Label.INPUT_OPERATOR, inputOperator.getExplainer(context));
        atts.put(Label.INPUT_OPERATOR, onPositive.getExplainer(context));
        for (TPreparedExpression field : tFields) {
            atts.put(Label.EXPRESSIONS, field.getExplainer(context));
        }
        atts.put(Label.PIPELINE,PrimitiveExplainer.getInstance(pipeline));
        return new CompoundExplainer(Type.BLOOM_FILTER, atts);
    }

    // Inner classes

    private interface ExpressionAdapter<EXPR,EVAL> {
        EVAL evaluate(EXPR expression, QueryContext contex);
        int hash(StoreAdapter adapter, EVAL evaluation, Row row, AkCollator collator);
    }

    private static ExpressionAdapter<TPreparedExpression, TEvaluatableExpression> newExpressionsAdapter
            = new ExpressionAdapter<TPreparedExpression, TEvaluatableExpression>() {
        @Override
        public TEvaluatableExpression evaluate(TPreparedExpression expression, QueryContext contex) {
            TEvaluatableExpression eval = expression.build();
            eval.with(contex);
            return eval;
        }

        @Override
        public int hash(StoreAdapter adapter, TEvaluatableExpression evaluation, Row row, AkCollator collator) {
            evaluation.with(row);
            evaluation.evaluate();
            return ValueSources.hash(evaluation.resultValue(), collator);
        }
    };

    private class Execution<E> extends ChainedCursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                super.open();
                filter = bindings.getBloomFilter(bindingPosition);
                bindings.setBloomFilter(bindingPosition, null);
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
                Row row;
                do {
                    row = input.next();
                    if (row == null) {
                        setIdle();
                    } else if (!filter.maybePresent(hashProjectedRow(row)) || !rowReallyHasMatch(row)) {
                        row = null;
                    }
                } while (isActive() && row == null);
                if (LOG_EXECUTION) {
                    LOG.debug("Select_BloomFilter: yield {}", row);
                }
                return row;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }


        // Execution interface

        <EXPR> Execution(QueryContext context, QueryBindingsCursor bindingsCursor,
                              List<? extends EXPR> expressions, ExpressionAdapter<EXPR,E> adapter)
        {
            super(context, inputOperator.cursor(context, bindingsCursor));
            this.onPositiveBindingsCursor = new SingletonQueryBindingsCursor(null);
            this.onPositiveCursor = onPositive.cursor(context, onPositiveBindingsCursor);
            this.adapter = adapter;
            for (EXPR field : expressions) {
                E eval = adapter.evaluate(field, context);
                fieldEvals.add(eval);
            }
        }

        // For use by this class

        private int hashProjectedRow(Row row)
        {
            int hash = 0;
            for (int f = 0; f < fieldEvals.size(); f++) {
                E fieldEval = fieldEvals.get(f);
                hash = hash ^ adapter.hash(adapter(), fieldEval, row, collator(f));
            }
            return hash;
        }

        private boolean rowReallyHasMatch(Row row)
        {
            // bindingPosition is used to hold onto a row for use during the evaluation of expressions
            // during onPositiveCursor.open(). This is somewhat sleazy, but the alternative is to
            // complicate the Select_BloomFilter API, requiring the caller to specify another binding position.
            // It is safe to reuse the binding position in this way because the filter is extracted and stored
            // in a field during open(), while the use of the binding position for use in the onPositive lookup
            // occurs during next().
            if (LOG_EXECUTION) {
                LOG.debug("Select_BloomFilter: candidate {}", row);
            }
            TAP_CHECK.in();
            try {
                bindings.setRow(bindingPosition, row);
                onPositiveBindingsCursor.reset(bindings);
                onPositiveCursor.openTopLevel();
                try {
                    return onPositiveCursor.next() != null;
                } finally {
                    onPositiveCursor.closeTopLevel();
                    bindings.setRow(bindingPosition, null);
                }
            } finally {
                TAP_CHECK.out();
            }
        }

        // Object state

        private final Cursor onPositiveCursor;
        private final SingletonQueryBindingsCursor onPositiveBindingsCursor;
        private BloomFilter filter;
        private final List<E> fieldEvals = new ArrayList<>();
        private final ExpressionAdapter<?, E> adapter;
    }

    // Turn input rows that match the filter into bindings for the onPositive plan.
    private class FilterBindingsCursor extends Map_NestedLoops.RowToBindingsCursor
    {
        private final StoreAdapter storeAdapter;
        private final List<TEvaluatableExpression> fieldEvals = new ArrayList<>();
        private final ExpressionAdapter<TPreparedExpression, TEvaluatableExpression> expressionAdapter;

        public FilterBindingsCursor(QueryContext context, Cursor input, 
                                    int bindingPosition, int depth,
                                    List<? extends TPreparedExpression> expressions, ExpressionAdapter<TPreparedExpression, TEvaluatableExpression> expressionAdapter) {
            super(input, bindingPosition, depth);
            this.storeAdapter = context.getStore();
            this.expressionAdapter = expressionAdapter;
            for (TPreparedExpression field : expressions) {
                TEvaluatableExpression eval = expressionAdapter.evaluate(field, context);
                fieldEvals.add(eval);
            }
        }

        @Override
        protected Row nextInputRow() {
            BloomFilter filter = baseBindings.getBloomFilter(bindingPosition);
            while (true) {
                Row row = input.next();
                if (row == null) {
                    return row;
                }
                if (filter.maybePresent(hashProjectedRow(row))) {
                    if (ExecutionBase.LOG_EXECUTION) {
                        LOG.debug("Select_BloomFilter: candidate {}", row);
                    }
                    return row;
                }
            }
        }

        private int hashProjectedRow(Row row)
        {
            int hash = 0;
            for (int f = 0; f < fieldEvals.size(); f++) {
                TEvaluatableExpression fieldEval = fieldEvals.get(f);
                hash = hash ^ expressionAdapter.hash(storeAdapter, fieldEval, row, collator(f));
            }
            return hash;
        }
    }

    // If any context at our depth has a non-empty rowset from
    // onPositive, it passed, so let it through.
    private static class RecoverRowsCursor extends Map_NestedLoops.CollapseBindingsCursor
    {
        private final int bindingPosition;

        public RecoverRowsCursor(QueryContext context, Cursor input, int bindingPosition, int depth) {
            super(context, input, depth);
            this.bindingPosition = bindingPosition;
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
                checkQueryCancelation();
                Row row = null;
                while (true) {
                    QueryBindings bindings = input.nextBindings();
                    if (bindings == null) {
                        openBindings = null;
                        break;
                    }
                    if (bindings.getDepth() < depth) {
                        pendingBindings = bindings;
                        openBindings = null;
                        break;
                    }
                    assert (bindings.getDepth() == depth);
                    input.open();
                    inputOpenBindings = bindings;
                    row = input.next();
                    input.close();
                    inputOpenBindings = null;
                    if (row != null) {
                        row = bindings.getRow(bindingPosition);
                        break;
                    }
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Select_BloomFilter: yield {}", row);
                }
                return row;
            } 
            finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }
    }
}
