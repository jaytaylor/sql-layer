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

package com.akiban.server.types3.texpressions;

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValueTarget;

public abstract class TQueryContextExpression implements TPreparedExpression {
    
    protected abstract void evaluate(QueryContext context, PValueTarget target);
    
    @Override
    public TPreptimeValue evaluateConstant() {
        return null;
    }

    @Override
    public TEvaluatableExpression build() {
        return new Evaluation();
    }
    
    private class Evaluation extends ContextualEvaluation<QueryContext> {

        @Override
        protected void evaluate(QueryContext context, PValueTarget target) {
            TQueryContextExpression.this.evaluate(context, target);
        }

        @Override
        public void with(QueryContext context) {
            setContext(context);
        }

        private Evaluation() {
            super(resultType().typeClass().underlyingType());
        }
    }
}
