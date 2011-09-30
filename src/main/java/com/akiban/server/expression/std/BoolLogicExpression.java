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
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.BooleanExtractor;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.BoolValueSource;

import java.util.List;

public final class BoolLogicExpression extends AbstractTwoArgExpression {

    // AbstractTwoArgExpression interface

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(logic.name());
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InternalEvaluation(logic, childrenEvaluations());
    }

    // private ctor -- the composers will be exposed as package-private

    private BoolLogicExpression(BooleanLogic logic, List<? extends Expression> children) {
        super(AkType.BOOL, children);
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
    static final ExpressionComposer andComposer = new InternalComposer(andLogic);
    static final ExpressionComposer orComposer = new InternalComposer(orLogic);

    // nested classes

    private static abstract class BooleanLogic {
        abstract boolean exec(boolean a, boolean b);
        Boolean nullTrumper() {
            return nullTrumper;
        }
        String name() {
            return name;
        }

        protected BooleanLogic(String name, boolean nullTrumper) {
            this.nullTrumper = nullTrumper;
            this.name = name;
        }

        private final Boolean nullTrumper;
        private final String name;
    }

    private static class InternalComposer implements ExpressionComposer {

        @Override
        public Expression compose(List<? extends Expression> arguments) {
            return new BoolLogicExpression(logic, arguments);
        }

        private InternalComposer(BooleanLogic logic) {
            this.logic = logic;
        }

        private final BooleanLogic logic;
    }

    private static class InternalEvaluation extends AbstractTwoArgExpressionEvaluation {

        @Override
        public ValueSource eval() {
            Boolean left = extractor.getBoolean(left(), null);
            Boolean right = extractor.getBoolean(right(), null);
            final Boolean result;
            if (left == null || right == null) {
                Boolean nullTrumper = logic.nullTrumper();
                result = (left == nullTrumper || right == nullTrumper) ? nullTrumper : null;
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
