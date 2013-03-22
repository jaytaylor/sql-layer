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

package com.akiban.server.expression.std;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;

public abstract class AbstractUnaryExpressionEvaluation extends ExpressionEvaluation.Base
{
    @Override
    public void of(Row row) {
        operandEvaluation.of(row);
    }

    @Override
    public void of(QueryContext context) {
        operandEvaluation.of(context);
        this.context = context;
    }

    @Override
    public void destroy() {
        operandEvaluation.destroy();
    }

    @Override
    public void acquire() {
        operandEvaluation.acquire();
    }

    @Override
    public boolean isShared() {
        return operandEvaluation.isShared();
    }

    @Override
    public void release() {
        operandEvaluation.release();
    }

    // for use by subclasses
    protected final ValueSource operand() {
        return operandEvaluation.eval();
    }

    protected final ExpressionEvaluation operandEvaluation() {
        return operandEvaluation;
    }

    protected AbstractUnaryExpressionEvaluation(ExpressionEvaluation operandEvaluation) {
        this.operandEvaluation = operandEvaluation;
    }

    protected ValueHolder valueHolder(){
        return valueHolder == null ? valueHolder = new ValueHolder() : valueHolder;
    }
    
    protected QueryContext queryContext() {
        return context;
    }
    
    // object state
    private final ExpressionEvaluation operandEvaluation;
    private ValueHolder valueHolder;
    private QueryContext context;
}
