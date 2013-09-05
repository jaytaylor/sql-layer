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
import com.foundationdb.server.explain.Attributes;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.explain.Type;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class Buffer_Default extends Operator
{

    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        return new Execution(context, inputOperator.cursor(context, bindingsCursor));
    }

    // Plannable interface

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
    }

    @Override
    public List<Operator> getInputOperators() {
        return Collections.singletonList(inputOperator);
    }

    @Override
    public String describePlan() {
        return super.describePlan();
    }

    // Object interface

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getClass().getSimpleName());
        str.append("(");
        str.append(inputOperator);
        str.append(")");
        return str.toString();
    }

    // Limit_Default interface

    Buffer_Default(Operator inputOperator) {
        this.inputOperator = inputOperator;
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Buffer_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Buffer_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(Buffer_Default.class);

    // Object state

    private final Operator inputOperator;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.INPUT_OPERATOR, inputOperator.getExplainer(context));
        return new CompoundExplainer(Type.BUFFER_OPERATOR, atts);
    }

    // internal classes

    private class Execution extends ChainedCursor {

        // Cursor interface

        @Override
        public void open() {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                super.open();
                closed = false;
                for(Row row : buffer) {
                    row.release();
                }
                buffer.clear();
                // Eager load input
                Row row;
                while((row = input.next()) != null) {
                    row.acquire();
                    buffer.add(row);
                }
                bufferIndex = 0;
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
                if(bufferIndex == buffer.size()) {
                    return null;
                }
                return buffer.get(bufferIndex++);
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
                super.close();
                closed = true;
            }
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

        // Execution interface
        Execution(QueryContext context, Cursor input) {
            super(context, input);
        }

        // object state

        private boolean closed = true;
        private int bufferIndex = 0;
        private final List<Row> buffer = new ArrayList<>();
    }
}
