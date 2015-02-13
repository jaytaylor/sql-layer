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

package com.foundationdb.server.types.texpressions;

import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.explain.std.TExpressionExplainer;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.value.ValueTargets;

public final class TPreparedField implements TPreparedExpression {
    
    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        return null;
    }

    @Override
    public TInstance resultType() {
        return typeInstance;
    }

    @Override
    public TEvaluatableExpression build() {
        return new Evaluation(typeInstance, fieldIndex);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new TExpressionExplainer(Type.FIELD, "Field", context);
        ex.addAttribute(Label.POSITION, PrimitiveExplainer.getInstance(fieldIndex));
        if (context.hasExtraInfo(this))
            ex.get().putAll(context.getExtraInfo(this).get());
        return ex;
    }

    @Override
    public String toString() {
        return "Field(" + fieldIndex + ')';
    }

    @Override
    public boolean isLiteral() {
        return false;
    }

    public TPreparedField(TInstance typeInstance, int fieldIndex) {
        this.typeInstance = typeInstance;
        this.fieldIndex = fieldIndex;
    }

    private final TInstance typeInstance;
    private final int fieldIndex;
    
    private static class Evaluation extends ContextualEvaluation<Row> {
        @Override
        protected void evaluate(Row context, ValueTarget target) {
            ValueSource rowSource = context.value(fieldIndex);
            ValueTargets.copyFrom(rowSource, target);
        }

        @Override
        public void with(Row row) {
            setContext(row);
        }

        private Evaluation(TInstance underlyingType, int fieldIndex) {
            super(underlyingType);
            this.fieldIndex = fieldIndex;
        }

        private int fieldIndex;
    }
}
