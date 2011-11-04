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

import com.akiban.server.error.DivisionByZeroException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;

public class LongOps {

    @Scalar("times")
    public static final LongOpForLong LONG_MULTIPLY = new LongOpForLong('*') {
        @Override
        public long evaluate(long one, long two) {
            return one * two;
        }
    };

    @Scalar("minus")
    public static final LongOpForLong LONG_SUBTRACT = new LongOpForLong('-') {
        @Override
        public long evaluate(long one, long two) {
            return one - two;
        }
    };

    @Scalar("plus")
    public static final LongOpForLong LONG_ADD = new LongOpForLong('+') {
        @Override
        public long evaluate(long one, long two) {
            return one + two;
        }
    };

    @Scalar("divide")
    public static final LongOpForLong LONG_DIVIDE = new LongOpForLong('/') {
        @Override
        public long evaluate(long one, long two) {
            if (two == 0)
                throw new DivisionByZeroException();
            return one / two;
        }
    };

    private LongOps() {}

    static abstract class LongOpForLong extends BinaryComposer implements LongOp {

        @Override
        protected Expression compose(Expression first, Expression second) {
            return new LongOpExpression(first, this, second);
        }

        @Override
        public AkType argumentType(int index) {
            return opType();
        }

        @Override
        protected ExpressionType composeType(ExpressionType first, ExpressionType second) {
            return ExpressionTypes.LONG;
        }

        @Override
        public AkType opType() {
            return AkType.LONG;
        }

        @Override
        public String toString() {
            return name;
        }

        protected LongOpForLong(char name) {
            this.name = String.format("op(%s)", name);
        }

        private final String name;
    }
}
