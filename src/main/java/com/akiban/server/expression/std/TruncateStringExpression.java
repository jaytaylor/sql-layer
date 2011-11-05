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

import com.akiban.server.error.OverflowException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;

public class TruncateStringExpression extends AbstractUnaryExpression
{
    public TruncateStringExpression(int length, Expression operand) {
        super(AkType.DECIMAL, operand);
        this.length = length;
    }

    @Override
    protected String name() {
        return "TruncateString";
    }

    @Override
    public String toString() {
        return name() + "(" + operand() + "," + length + ")";
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(length, operandEvaluation());
    }

    private final int length;

    private static final class InnerEvaluation extends AbstractUnaryExpressionEvaluation {
        public InnerEvaluation(int length, ExpressionEvaluation ev) {
            super(ev);
            this.length = length;
        }

        @Override
        public ValueSource eval() {
            ValueSource operandSource = operand();
            if (operandSource.isNull())
                return NullValueSource.only();
            String value = operandSource.getString();
            if (value.length() > length)
                value = value.substring(0, length);
            return hold(value);
        }

        private ValueSource hold(String value) {
            if (holder == null) {
                holder = new ValueHolder();
            }
            holder.putString(value);
            return holder;
        }

        private final int length;
        private ValueHolder holder;
    }
}
