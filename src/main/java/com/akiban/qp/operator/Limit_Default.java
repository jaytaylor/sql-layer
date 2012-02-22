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
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.tap.InOutTap;
import com.akiban.server.error.NegativeLimitException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;

import com.akiban.sql.optimizer.explain.Attributes;
import com.akiban.sql.optimizer.explain.Label;
import com.akiban.sql.optimizer.explain.OperationExplainer;
import com.akiban.sql.optimizer.explain.PrimitiveExplainer;
import com.akiban.sql.optimizer.explain.Type;
import java.math.BigDecimal;
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
    protected Cursor cursor(QueryContext context) {
        return new Execution(context, inputOperator.cursor(context));
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
    
    // Object state

    private final int skip, limit;
    private final boolean skipIsBinding, limitIsBinding;
    private final Operator inputOperator;

    @Override
    public Explainer getExplainer()
    {
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
        
        
        Attributes att = new Attributes();
        att.put(Label.NAME, PrimitiveExplainer.getInstance("LIMIT DEFAULT"));
        if (skip > 0)
            att.put(Label.LIMIT, PrimitiveExplainer.getInstance(String.format("skip = %d", skip)));
        if (limit >= 0 && limit < Integer.MAX_VALUE)
            att.put(Label.LIMIT, PrimitiveExplainer.getInstance(String.format("limit = %d", limit)));
        att.put(Label.INPUT_OPERATOR, inputOperator.getExplainer());
        
        return new OperationExplainer(Type.LIMIT_OPERATOR, att);
    }

    // internal classes

    private class Execution extends ChainedCursor {

        // Cursor interface

        @Override
        public void open() {
            TAP_OPEN.in();
            try {
                super.open();
                if (isSkipBinding()) {
                    ValueSource value = context.getValue(skip());
                    if (!value.isNull())
                        this.skipLeft = (int)Extractors.getLongExtractor(AkType.LONG).getLong(value);
                }
                else {
                    this.skipLeft = skip();
                }
                if (skipLeft < 0)
                    throw new NegativeLimitException("OFFSET", skipLeft);
                if (isLimitBinding()) {
                    ValueSource value = context.getValue(limit());
                    if (value.isNull())
                        this.limitLeft = Integer.MAX_VALUE;
                    else
                        this.limitLeft = (int)Extractors.getLongExtractor(AkType.LONG).getLong(value);
                }
                else {
                    this.limitLeft = limit();
                }
                if (limitLeft < 0)
                    throw new NegativeLimitException("LIMIT", limitLeft);
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next() {
            TAP_NEXT.in();
            try {
                checkQueryCancelation();
                Row row;
                while (skipLeft > 0) {
                    if ((row = input.next()) == null) {
                        skipLeft = 0;
                        limitLeft = -1;
                        return null;
                    }
                    skipLeft--;
                }
                if (limitLeft < 0) {
                    return null;
                }
                if (limitLeft == 0) {
                    input.close();
                    return null;
                }
                if ((row = input.next()) == null) {
                    limitLeft = -1;
                    return null;
                }
                --limitLeft;
                return row;
            } finally {
                TAP_NEXT.out();
            }
        }

        // Execution interface
        Execution(QueryContext context, Cursor input) {
            super(context, input);
        }

        // class state

        private int skipLeft, limitLeft;
    }
}
