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
import com.foundationdb.server.error.NegativeLimitException;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**

 <h1>Overview</h1>

 Provides a very simple, count-based limit.

 <h1>Arguments</h1>

 <ul>

 <li><b>PhysicalOperator inputOperator:</b> The input operator, whose
 cursor will be limited

 <li><b>int limit:</b>&nbsp;the number of rows to limit to; cannot be
 negative

 </ul>

 <h1>Behavior</h1>

 Limit_Default's cursor returns at most <i>limit</i> rows from the input
 operator's cursor.  When the limit is reached, the input cursor is
 automatically closed, and all subsequent calls to <i>next</i> will return
 <i>false</i>.

 <h1>Output</h1>

 Nothing else to say.

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 Essentially free.

 <h1>Memory Requirements</h1>

 None.

 */

final class Limit_Default extends Operator
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
        if (skip > 0) {
            str.append(String.format("skip=%d", skip));
        }
        if ((limit >= 0) && (limit < Integer.MAX_VALUE)) {
            if (skip > 0) str.append(", ");
            str.append(String.format("limit=%d", limit));
        }
        str.append(": ");
        str.append(inputOperator);
        str.append(")");
        return str.toString();
    }

    // Limit_Default interface

    Limit_Default(Operator inputOperator, int limit) {
        this(inputOperator, 0, false, limit, false);
    }

    Limit_Default(Operator inputOperator,
                  int skip, boolean skipIsBinding,
                  int limit, boolean limitIsBinding) {
        ArgumentValidation.isGTE("skip", skip, 0);
        ArgumentValidation.isGTE("limit", limit, 0);
        this.skip = skip;
        this.skipIsBinding = skipIsBinding;
        this.limit = limit;
        this.limitIsBinding = limitIsBinding;
        this.inputOperator = inputOperator;
    }

    public int skip() {
        return skip;
    }
    public boolean isSkipBinding() {
        return skipIsBinding;
    }
    public int limit() {
        return limit;
    }
    public boolean isLimitBinding() {
        return limitIsBinding;
    }
    
    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Limit_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Limit_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(Limit_Default.class);

    // Object state

    private final int skip, limit;
    private final boolean skipIsBinding, limitIsBinding;
    private final Operator inputOperator;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.LIMIT, PrimitiveExplainer.getInstance(limit));
        atts.put(Label.INPUT_OPERATOR, inputOperator.getExplainer(context));
        return new CompoundExplainer(Type.LIMIT_OPERATOR, atts);
    }

    // internal classes

    private class Execution extends ChainedCursor {

        // Cursor interface

        @Override
        public void open() {
            TAP_OPEN.in();
            try {
                if (isSkipBinding()) {
                    ValueSource value = bindings.getValue(skip());
                    if (!value.isNull())
                        this.skipLeft = value.getInt32();
                }
                else {
                    this.skipLeft = skip();
                }
                if (skipLeft < 0)
                    throw new NegativeLimitException("OFFSET", skipLeft);
                if (isLimitBinding()) {
                    ValueSource value = bindings.getValue(limit());
                    if (value.isNull())
                        this.limitLeft = Integer.MAX_VALUE;
                    else {
                        TInstance type = MNumeric.INT.instance(true);
                        TExecutionContext executionContext = 
                            new TExecutionContext(null, type, context);
                        Value ivalue = new Value(MNumeric.INT.instance(true));
                        MNumeric.INT.fromObject(executionContext, value, ivalue);
                        this.limitLeft = ivalue.getInt32();
                    }
                }
                else {
                    this.limitLeft = limit();
                }
                if (limitLeft < 0)
                    throw new NegativeLimitException("LIMIT", limitLeft);
                super.open();
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override 
        public void close() {
            // Because the checks in open() may 
            // prevent the cursor from being opened. 
            if (!isClosed()) {
                super.close();
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
                checkQueryCancelation();
                Row row;
                while (skipLeft > 0) {
                    if ((row = input.next()) == null) {
                        skipLeft = 0;
                        limitLeft = -1;
                        setIdle();
                        if (LOG_EXECUTION) {
                            LOG.debug("Limit_Default: skipLeft until complete yield null");
                        }
                        return null;
                    }
                    skipLeft--;
                }
                if (limitLeft < 0) {
                    setIdle();
                    if (LOG_EXECUTION) {
                        LOG.debug("Limit_Default: limitLeft < 0, yield null");
                    }
                    return null;
                }
                if (limitLeft == 0) {
                    setIdle();
                    if (LOG_EXECUTION) {
                        LOG.debug("Limit_Default: limitLeft == 0, yield null");
                    }
                    return null;
                }
                if ((row = input.next()) == null) {
                    limitLeft = -1;
                    setIdle();
                    if (LOG_EXECUTION) {
                        LOG.debug("Limit_Default: yield null");
                    }
                    return null;
                }
                --limitLeft;
                if (LOG_EXECUTION) {
                    LOG.debug("Limit_Default: yield {}", row);
                }
                return row;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        // Execution interface
        Execution(QueryContext context, Cursor input) {
            super(context, input);
        }

        // object state

        private int skipLeft, limitLeft;
    }
}
