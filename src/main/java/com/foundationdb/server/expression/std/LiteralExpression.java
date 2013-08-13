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

package com.foundationdb.server.expression.std;

import com.foundationdb.qp.exec.Plannable;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.explain.Explainer;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.explain.Type;
import com.foundationdb.server.explain.std.ExpressionExplainer;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.util.BoolValueSource;
import com.foundationdb.server.types.util.SqlLiteralValueFormatter;
import com.foundationdb.server.types.util.ValueHolder;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class LiteralExpression implements Expression {

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public boolean needsBindings() {
        return false;
    }

    @Override
    public boolean needsRow() {
        return false;
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return  evaluation;
    }

    @Override
    public AkType valueType() {
        return evaluation.eval().getConversionType();
    }

    public LiteralExpression(ValueSource source) {
        this(new InternalEvaluation(new ValueHolder(source)));
    }

    public LiteralExpression(AkType type, long value) throws ValueHolder.IllegalRawPutException {
        this(type == AkType.NULL ? NULL_EVAL :new InternalEvaluation(new ValueHolder(type, value)));
    }

    public LiteralExpression(AkType type, double value) throws ValueHolder.IllegalRawPutException {
        this(type == AkType.NULL ? NULL_EVAL :new InternalEvaluation(new ValueHolder(type, value)));
    }

    public LiteralExpression(AkType type, float value) throws ValueHolder.IllegalRawPutException {
        this(type == AkType.NULL ? NULL_EVAL :new InternalEvaluation(new ValueHolder(type, value)));
    }

    public LiteralExpression(AkType type, boolean value) throws ValueHolder.IllegalRawPutException {
        this(type == AkType.NULL ? NULL_EVAL :new InternalEvaluation(new ValueHolder(type, value)));
    }

    public LiteralExpression(AkType type, Object value) throws ValueHolder.IllegalRawPutException {
        this(type == AkType.NULL ? NULL_EVAL :new InternalEvaluation(new ValueHolder(type, value)));
    }
    
    private LiteralExpression(InternalEvaluation evaluation) {
        this.evaluation = evaluation;
      }
    
    public static Expression forNull() {
        return NULL_EXPRESSION;
    }

    public static Expression forBool(Boolean value) {
        if (value == null)
            return BOOL_NULL;
        return value ? BOOL_TRUE : BOOL_FALSE;
    }

    // Object interface

    @Override
    public String toString() {
        return "Literal(" + evaluation.eval().toString() + ')';
    }

    // object state

    private final ExpressionEvaluation evaluation;
    
    // const

    private static final InternalEvaluation NULL_EVAL  = new InternalEvaluation(NullValueSource.only()) ;
    private static final Expression NULL_EXPRESSION = new LiteralExpression(NULL_EVAL);
    private static final Expression BOOL_TRUE = new LiteralExpression(new InternalEvaluation(BoolValueSource.OF_TRUE));
    private static final Expression BOOL_FALSE = new LiteralExpression(new InternalEvaluation(BoolValueSource.OF_FALSE));
    private static final Expression BOOL_NULL = new LiteralExpression(new InternalEvaluation(BoolValueSource.OF_NULL));

    @Override
    public String name()
    {
        return "LITERAL";
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new ExpressionExplainer(Type.LITERAL, name(), context);
        String sql = SqlLiteralValueFormatter.format(evaluation.eval());
        ex.addAttribute(Label.OPERAND, PrimitiveExplainer.getInstance(sql));
        return ex;
    }
    
    public boolean nullIsContaminating()
    {
        return true;
    }
    
    // nested classes
    
    private static class InternalEvaluation extends ExpressionEvaluation.Base {
        @Override
        public void of(Row row) {
        }

        @Override
        public void of(QueryContext context) {
        }

        @Override
        public void of(QueryBindings bindings) {
        }

        @Override
        public ValueSource eval() {
            return valueSource;
        }

        @Override
        public void acquire() {
        }

        @Override
        public boolean isShared() {
            return false;
        }

        @Override
        public void release() {
        }

        private InternalEvaluation(ValueSource valueSource) {
            this.valueSource = valueSource;
        }

        private final ValueSource valueSource;
    }
}
