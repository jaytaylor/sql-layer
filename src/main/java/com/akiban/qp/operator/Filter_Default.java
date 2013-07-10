/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.qp.operator;

import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.std.FilterExplainer;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**

 <h1>Overview</h1>

 Extract_Default filters the input stream, keeping rows that match or are descendents of a given type, and discarding others.

 <h1>Arguments</h1>

 <ul>
 <li><b>Collection<RowType> extractTypes:</b> Specifies types of rows to be passed on to the output stream. 
</ul>
 
 <h1>Behavior</h1>

 A row is kept if its type matches one of the extractTypes, or is a descendent of one of these types. Other rows are discarded.

 <h1>Output</h1>

 Nothing else to say.

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 Extract_Default does no IO. For each input row, the type is checked and the row is either kept (written to the output stream) or discarded.

 <h1>Memory Requirements</h1>

 None.

 
 */

class Filter_Default extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        TreeSet<String> keepTypesStrings = new TreeSet<>();
        for (RowType keepType : keepTypes) {
            keepTypesStrings.add(String.valueOf(keepType));
        }
        return String.format("%s(%s)", getClass().getSimpleName(), keepTypesStrings);
    }

    // Operator interface

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
    }

    @Override
    public List<Operator> getInputOperators()
    {
        return Collections.singletonList(inputOperator);
    }

    @Override
    protected Cursor cursor(QueryContext context, QueryBindings bindings)
    {
        return new Execution(context, bindings, inputOperator.cursor(context, bindings));
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Filter_Default interface

    public Filter_Default(Operator inputOperator, Collection<? extends RowType> keepTypes)
    {
        ArgumentValidation.notEmpty("keepTypes", keepTypes);
        this.inputOperator = inputOperator;
        this.keepTypes = new HashSet<>(keepTypes);
    }
    
    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Filter_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Filter_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(Filter_Default.class);

    // Object state

    private final Operator inputOperator;
    private final Set<RowType> keepTypes;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        return new FilterExplainer(getName(), keepTypes, inputOperator, context);
    }

    // Inner classes

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
                closed = false;
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
                Row row;
                do {
                    row = input.next();
                    if (row == null) {
                        close();
                    } else if (!keepTypes.contains(row.rowType())) {
                        row = null;
                    }
                } while (row == null && !closed);
                if (LOG_EXECUTION) {
                    LOG.debug("Filter_Default: yield {}", row);
                }
                return row;
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
            if (!closed) {
                input.close();
                closed = true;
            }
        }

        @Override
        public void destroy()
        {
            input.destroy();
        }

        @Override
        public boolean isIdle()
        {
            return closed;
        }

        @Override
        public boolean isActive()
        {
            return !closed;
        }

        @Override
        public boolean isDestroyed()
        {
            return input.isDestroyed();
        }

        // Execution interface

        Execution(QueryContext context, QueryBindings bindings, Cursor input)
        {
            super(context, bindings);
            this.input = input;
        }

        // Object state

        private final Cursor input;
        private boolean closed = true;
    }
}
