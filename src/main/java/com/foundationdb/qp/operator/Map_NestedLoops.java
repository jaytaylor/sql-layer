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
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.explain.std.NestedLoopsExplainer;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**

 <h1>Overview</h1>

 Map_NestedLoops implements a mapping using a nested-loop algorithm. The left input operator (outer loop)
 provides a stream of input rows. The right input operator (inner loop) binds this row, and 
 its output, combined across all input rows, forms the Map_NestedLoops output stream. 
 
 <h1>Arguments</h1>

 <ul>

 <li><b>Operator outerInputOperator:</b> Provides stream of input.

 <li><b>Operator innerInputOperator:</b> Provides Map_NestedLoops output.

 <li><b>int inputBindingPosition:</b> Position of inner loop row in query context.

 <li><b>boolean pipeline:</b> Whether to use bracketing cursors instead of rebinding.

 <li><b>int depth:</b> Number of nested Maps, including this one.

 </ul>

 <h1>Behavior</h1>

 The outer input operator provides a stream of rows. Each is bound in turn to the query context, in the 
 position specified by inputBindingPosition. For each input row, the inner operator binds the row and executes,
 yielding a stream of output rows. The concatenation of these streams comprises the output from Map_NestedLoops.
 
 The inner operator is used multiple times, once for each input row. On each iteration, the input row is bound
 to the query context, the inner cursor is opened, and then the inner cursor is consumed.

 <h1>Output</h1>

 The concatenation of streams from the inner operator.

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 Map_NestedLoops does no IO.

 <h1>Memory Requirements</h1>

 A single row from the outer loops is stored at all times.
 
 */


class Map_NestedLoops extends Operator
{
    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        if (!pipeline)
            return new Execution(context, bindingsCursor); // Old-style
        else {
            Cursor outerCursor = outerInputOperator.cursor(context, bindingsCursor);
            QueryBindingsCursor toBindings = new RowToBindingsCursor(outerCursor, inputBindingPosition, depth);
            Cursor innerCursor = innerInputOperator.cursor(context, toBindings);
            return new CollapseBindingsCursor(context, innerCursor, depth);
        }
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        innerInputOperator.findDerivedTypes(derivedTypes);
        outerInputOperator.findDerivedTypes(derivedTypes);
    }

    @Override
    public List<Operator> getInputOperators()
    {
        List<Operator> result = new ArrayList<>(2);
        result.add(outerInputOperator);
        result.add(innerInputOperator);
        return result;
    }

    @Override
    public String describePlan()
    {
        return String.format("%s\n%s", describePlan(outerInputOperator), describePlan(innerInputOperator));
    }

    // Map_NestedLoops interface

    public Map_NestedLoops(Operator outerInputOperator,
                           Operator innerInputOperator,
                           int inputBindingPosition,
                           boolean pipeline,
                           int depth)
    {
        ArgumentValidation.notNull("outerInputOperator", outerInputOperator);
        ArgumentValidation.notNull("innerInputOperator", innerInputOperator);
        ArgumentValidation.isGTE("inputBindingPosition", inputBindingPosition, 0);
        if (pipeline)
            ArgumentValidation.isGT("depth", depth, 0);
        this.outerInputOperator = outerInputOperator;
        this.innerInputOperator = innerInputOperator;
        this.inputBindingPosition = inputBindingPosition;
        this.pipeline = pipeline;
        this.depth = depth;
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Map_NestedLoops open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Map_NestedLoops next");
    private static final Logger LOG = LoggerFactory.getLogger(Map_NestedLoops.class);

    // Object state

    private final Operator outerInputOperator;
    private final Operator innerInputOperator;
    private final int inputBindingPosition, depth;
    private final boolean pipeline;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new NestedLoopsExplainer(getName(), innerInputOperator, outerInputOperator, null, null, context);
        ex.addAttribute(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(inputBindingPosition));
        ex.addAttribute(Label.PIPELINE, PrimitiveExplainer.getInstance(pipeline));
        ex.addAttribute(Label.DEPTH, PrimitiveExplainer.getInstance(depth));
        if (context.hasExtraInfo(this))
            ex.get().putAll(context.getExtraInfo(this).get());
        return ex;
    }

    // Inner classes

    // Pipeline execution: turn outer loop row stream into binding stream for inner loop.
    protected static class RowToBindingsCursor implements QueryBindingsCursor
    {
        protected final Cursor input;
        protected final int depth, bindingPosition;
        protected QueryBindings baseBindings;

        public RowToBindingsCursor(Cursor input, int bindingPosition, int depth) {
            this.input = input;
            this.bindingPosition = bindingPosition;
            this.depth = depth;
        }

        @Override
        public void openBindings() {
            input.openBindings();
            baseBindings = null;
        }

        @Override
        public QueryBindings nextBindings() {
            if (baseBindings != null) {
                Row row = nextInputRow();
                if (row != null) {
                    if (row.isBindingsSensitive()) {
                        // Freeze values which may depend on outer bindings.
                        row = ImmutableRow.buildImmutableRow(row);
                    }
                    QueryBindings bindings = baseBindings.createBindings();
                    assert (bindings.getDepth() == depth);
                    bindings.setRow(bindingPosition, row);
                    return bindings;
                }
                baseBindings = null;
                input.close();
            }
            QueryBindings bindings = input.nextBindings();
            if ((bindings != null) && (bindings.getDepth() == depth - 1)) {
                // Outer context: start outer loop.
                baseBindings = bindings;
                input.open();
            }
            if (ExecutionBase.LOG_EXECUTION) {
                LOG.debug("Map_NestedLoops$RowToBindingsCursor: bindings {}", bindings);
            }
            
            return bindings;
        }

        @Override
        public void closeBindings() {
            if (baseBindings != null) {
                baseBindings = null;
                input.close();
            }
            input.closeBindings();
        }

        @Override
        public void cancelBindings(QueryBindings bindings) {
            if ((baseBindings != null) && baseBindings.isAncestor(bindings)) {
                baseBindings = null;
                input.close();
                input.cancelBindings(bindings);
            }
        }

        protected Row nextInputRow() {
            assert input.isActive() : "RowToBindingsCursor reading from idle cursor";
            Row row = input.next();
            return row;
        }
    }

    // Other end of pipeline: remove the extra binding levels that we
    // introduced, collapsing rowsets in between into one for the
    // entire outer rowset.
    protected static class CollapseBindingsCursor extends OperatorCursor
    {
        protected final Cursor input;
        protected final int depth;
        protected QueryBindings currentBindings, pendingBindings, openBindings, inputOpenBindings;
        
        public CollapseBindingsCursor(QueryContext context, Cursor input, int depth) {
            super(context);
            this.input = input;
            this.depth = depth;
        }

        @Override
        public void open() {
            super.open();
            openBindings = currentBindings;
            inputOpenBindings = null;
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
                    if (inputOpenBindings != null) {
                        row = input.next();
                        if (row != null) break;
                        input.close();
                        inputOpenBindings = null;
                    }
                    QueryBindings bindings = input.nextBindings();
                    if (bindings == null) {
                        openBindings = null;
                        break;
                    }
                    if (bindings.getDepth() == depth) {
                        input.open();
                        inputOpenBindings = bindings;
                    }
                    else if (bindings.getDepth() < depth) {
                        // End of this binding's rowset. Arrange for this to be next one.
                        pendingBindings = bindings;
                        openBindings = null;
                        break;
                    }
                    else {
                        assert false : "bindings too deep";
                    }
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Map_NestedLoops$CollapseBindingsCursor: yield {}", row);
                }
                return row;
            } 
            finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        @Override
        public void close() {
            // The input may be closed in next(), 
            // in anticipation of of a call to nextBinding()
            // which never arrives.
            if (!input.isClosed()) {
                input.close();
            }
            super.close();
            if (openBindings != null) {
                cancelBindings(openBindings); //TODO Close Bindings? Skip? 
                assert (inputOpenBindings == null);
                assert (openBindings == null);
            }
        }


        @Override
        public boolean isIdle() {
            return !input.isClosed() && (openBindings == null);
        }

        @Override
        public boolean isActive() {
            return (openBindings != null);
        }

        @Override
        public void openBindings() {
            pendingBindings = currentBindings = null;
            input.openBindings();
        }

        @Override
        public QueryBindings nextBindings() {
            currentBindings = pendingBindings;
            if (currentBindings != null) {
                pendingBindings = null;
                return currentBindings;
            }
            while (true) {
                // Skip over any that we would elide.
                currentBindings = input.nextBindings();
                if ((currentBindings == null) || (currentBindings.getDepth() < depth))
                    return currentBindings;
                assert (currentBindings.getDepth() == depth);
            }
        }

        @Override
        public void closeBindings() {
            input.closeBindings();
        }

        @Override
        public void cancelBindings(QueryBindings bindings) {
            CursorLifecycle.checkClosed(this);
            input.cancelBindings(bindings);
            if ((inputOpenBindings != null) && inputOpenBindings.isAncestor(bindings)) {
                inputOpenBindings = null;
            }
            if ((openBindings != null) && openBindings.isAncestor(bindings)) {
                openBindings = null;
            }
            if ((pendingBindings != null) && pendingBindings.isAncestor(bindings)) {
                pendingBindings = null;
            }
        }
    }

    // Old-style execution: bind outer row into existing context and
    // open inner loop afresh.
    private class Execution extends OperatorCursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                super.open();
                outerInput.open();
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
                Row outputRow = null;
                while (outerInput.isActive() && outputRow == null) {
                    outputRow = nextOutputRow();
                    if (outputRow == null) {
                        Row row = outerInput.next();
                        if (row == null) {
                            outerInput.setIdle();
                        } else {
                            outerRow = row;
                            if (LOG_EXECUTION) {
                                LOG.debug("Map_NestedLoops$Execution: restart inner loop using current branch row");
                            }
                            startNewInnerLoop(row);
                        }
                    }
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Map_NestedLoops$Execution: yield {}", outputRow);
                }
                return outputRow;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        @Override
        public void close()
        {
            super.close();
            if (!innerInput.isClosed())
                innerInput.closeTopLevel();
            closeOuter();
        }

        @Override
        public boolean isIdle()
        {
            return outerInput.isIdle();
        }

        @Override
        public boolean isActive()
        {
            return outerInput.isActive();
        }

        @Override
        public void openBindings() {
            outerInput.openBindings();
        }

        @Override
        public QueryBindings nextBindings() {
            outerBindings = outerInput.nextBindings();
            return outerBindings;
        }

        @Override
        public void closeBindings() {
            outerInput.closeBindings();
        }

        @Override
        public void cancelBindings(QueryBindings bindings) {
            CursorLifecycle.checkClosed(this);
            innerInput.close();
            outerInput.cancelBindings(bindings);
        }

        // Execution interface

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor)
        {
            super(context);
            this.outerInput = outerInputOperator.cursor(context, bindingsCursor);
            // For now, the inside sees whatever bindings the outside currently has.
            this.innerBindingsCursor = new SingletonQueryBindingsCursor(null);
            this.innerInput = innerInputOperator.cursor(context, innerBindingsCursor);
        }

        
        // For use by this class

        private Row nextOutputRow()
        {
            Row outputRow = null;
            if (outerRow != null) {
                Row innerRow = innerInput.next();
                if (innerRow == null) {
                    innerInput.setIdle();
                    outerRow = null;
                } else {
                    outputRow = innerRow;
                    if (outputRow.isBindingsSensitive()) {
                        // Freeze values which may depend on outer bindings.
                        outputRow = ImmutableRow.buildImmutableRow(outputRow);
                    }
                }
            }
            return outputRow;
        }

        private void closeOuter()
        {
            outerRow = null;
            outerInput.close();
        }

        private void startNewInnerLoop(Row row)
        {
            if (!innerInput.isClosed())
                innerInput.closeTopLevel();
            outerBindings.setRow(inputBindingPosition, row);
            innerBindingsCursor.reset(outerBindings);
            innerInput.openTopLevel();
        }

        // Object state

        private final Cursor outerInput;
        private final Cursor innerInput;
        private Row outerRow;
        private QueryBindings outerBindings;
        private final SingletonQueryBindingsCursor innerBindingsCursor;
    }
}
