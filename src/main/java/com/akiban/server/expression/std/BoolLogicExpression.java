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

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.BooleanExtractor;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.BoolValueSource;

import java.util.List;

public final class BoolLogicExpression extends AbstractBinaryExpression {

    // AbstractTwoArgExpression interface

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(logic.name());
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InternalEvaluation(logic, childrenEvaluations());
    }

    @Override
    protected boolean nullIsContaminating() {
        return false;
    }
    
    // private ctor -- the composers will be exposed as package-private

    private BoolLogicExpression(Expression lhs, BooleanLogic logic, Expression rhs) {
        super(AkType.BOOL, lhs, rhs);
        this.logic = logic;
    }

    // object state

    private final BooleanLogic logic;

    // class state / consts

    private static final BooleanExtractor extractor = Extractors.getBooleanExtractor();
    private static final BooleanLogic andLogic = new BooleanLogic("AND", false) {
        @Override
        public boolean exec(boolean a, boolean b) {
            return a && b;
        }
    };
    private static final BooleanLogic orLogic = new BooleanLogic("OR", true) {
        @Override
        public boolean exec(boolean a, boolean b) {
            return a || b;
        }
    };

    @Scalar("and")
    public static final ExpressionComposer andComposer = new InternalComposer(andLogic);

    @Scalar("or")
    public static final ExpressionComposer orComposer = new InternalComposer(orLogic);

    // nested classes

    private static abstract class BooleanLogic {
        abstract boolean exec(boolean a, boolean b);
        Boolean trump() {
            return trump;
        }
        String name() {
            return name;
        }

        protected BooleanLogic(String name, boolean trump) {
            this.trump = trump;
            this.name = name;
        }

        private final Boolean trump;
        private final String name;
    }

    private static class InternalComposer extends BinaryComposer{
        @Override
        protected Expression compose(Expression first, Expression second) {
            return new BoolLogicExpression(first, logic, second);
        }

        private InternalComposer(BooleanLogic logic) {
            this.logic = logic;
        }

        private final BooleanLogic logic;
    }

    private static class InternalEvaluation extends AbstractTwoArgExpressionEvaluation {

        @Override
        public ValueSource eval() {
            Boolean trump = logic.trump();
            Boolean left = extractor.getBoolean(left(), null);
            if (left == trump)
                return BoolValueSource.of(left);

            Boolean right = extractor.getBoolean(right(), null);
            final Boolean result;
            if (left == null || right == null) {
                result = (right == trump) ? trump : null; // lhs can't be trump; we'd have short-circuited already
            }
            else {
                result = logic.exec(left, right);
            }
            return BoolValueSource.of(result);
        }

        private InternalEvaluation(BooleanLogic logic, List<? extends ExpressionEvaluation> children) {
            super(children);
            this.logic = logic;
        }

        private final BooleanLogic logic;
    }
}
