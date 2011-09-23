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

package com.akiban.server.expression.std;

import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.row.Row;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;

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
        return evaluation;
    }

    @Override
    public AkType valueType() {
        return evaluation.eval().getConversionType();
    }

    public LiteralExpression(AkType type, long value) throws ValueHolder.IllegalRawPutException {
        this(new InternalEvaluation(new ValueHolder(type, value)));
    }

    public LiteralExpression(AkType type, double value) throws ValueHolder.IllegalRawPutException {
        this(new InternalEvaluation(new ValueHolder(type, value)));
    }

    public LiteralExpression(AkType type, float value) throws ValueHolder.IllegalRawPutException {
        this(new InternalEvaluation(new ValueHolder(type, value)));
    }

    public LiteralExpression(AkType type, Object value) throws ValueHolder.IllegalRawPutException {
        this(new InternalEvaluation(new ValueHolder(type, value)));
    }
    
    private LiteralExpression(InternalEvaluation evaluation) {
        this.evaluation = evaluation;
    }
    
    public static Expression forNull() {
        return NULL_EXPRESSION;
    }

    // Object interface

    @Override
    public String toString() {
        return evaluation.eval().toString();
    }

    // object state

    private final ExpressionEvaluation evaluation;
    
    // const
    
    private static Expression NULL_EXPRESSION = new LiteralExpression(new InternalEvaluation(ValueHolder.holdingNull()));

    // nested classes
    
    private static class InternalEvaluation implements  ExpressionEvaluation {
        @Override
        public void of(Row row) {
        }

        @Override
        public void of(Bindings bindings) {
        }

        @Override
        public ValueSource eval() {
            return valueSource;
        }

        @Override
        public void share() {
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
