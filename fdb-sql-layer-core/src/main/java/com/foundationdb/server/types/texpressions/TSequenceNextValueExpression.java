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

import com.foundationdb.ais.model.Sequence;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.explain.Attributes;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.explain.Type;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;

public class TSequenceNextValueExpression implements TPreparedExpression
{
    private final TInstance resultType;
    private final Sequence sequence;

    public TSequenceNextValueExpression(TInstance resultType, Sequence sequence) {
        assert resultType != null;
        assert sequence != null;
        this.resultType = resultType;
        this.sequence = sequence;
    }

    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        // Never constant
        return null;
    }

    @Override
    public TInstance resultType() {
        return resultType;
    }

    @Override
    public TEvaluatableExpression build() {
        return new InnerEvaluation();
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes states = new Attributes();
        states.put(Label.NAME, PrimitiveExplainer.getInstance("_SEQ_NEXT"));
        states.put(Label.OPERAND, PrimitiveExplainer.getInstance(sequence.getSequenceName().getSchemaName()));
        states.put(Label.OPERAND, PrimitiveExplainer.getInstance(sequence.getSequenceName().getTableName()));
        return new CompoundExplainer(Type.FUNCTION, states);
    }

    @Override
    public boolean isLiteral() {
        return false;
    }

    private class InnerEvaluation implements TEvaluatableExpression
    {
        private QueryContext context;
        private Value value;

        @Override
        public ValueSource resultValue() {
            return value;
        }

        @Override
        public void evaluate() {
            long next = context.getStore().sequenceNextValue(sequence);
            value = new Value(resultType);
            ValueSources.valueFromLong(next, value);
        }

        @Override
        public void with(Row row) {
        }

        @Override
        public void with(QueryContext context) {
            this.context = context;
            this.value = null;
        }

        @Override
        public void with(QueryBindings bindings) {
        }
    }
}
