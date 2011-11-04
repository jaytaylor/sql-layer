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

import com.akiban.server.Quote;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.util.AkibanAppender;

import java.util.List;

public final class ConcatExpression extends AbstractCompositeExpression {

    static class ConcatComposer implements ExpressionComposer {
        @Override
        public Expression compose(List<? extends Expression> arguments) {
            return new ConcatExpression(arguments);
        }

        @Override
        public AkType argumentType(int index) {
            return AkType.VARCHAR;
        }

        @Override
        public ExpressionType composeType(List<? extends ExpressionType> argumentTypes) {
            int length = 0;
            for (ExpressionType type : argumentTypes) {
                if (type.getType() == AkType.VARCHAR) {
                    length += type.getPrecision();
                }
            }
            return ExpressionTypes.varchar(length);
        }
    }

    @Scalar("concatenate")
    public static final ExpressionComposer COMPOSER = new ConcatComposer();
    
    @Scalar("concat")
    public static final ExpressionComposer COMPOSER_ALIAS = COMPOSER;

    @Override
    protected void describe(StringBuilder sb) {
        sb.append("CONCAT");
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(childrenEvaluations());
    }

    @Override
    public boolean isConstant() {
        boolean foundNonConst = false;
        for (Expression child : children()) {
            if (!child.isConstant()) {
                if (shortCircuitLazily)
                    return foundNonConst;
                foundNonConst = true;
            }
            else if (child.evaluation().eval().isNull())
                return true;
        }
        return ! foundNonConst;
    }

    /**
     * <p>Creates a CONCAT with a given aggressiveness of short-circuiting for const evaluation.</p>
     * <p>CONCAT is NULL if any of its arguments are NULL, so the default behavior of this expression
     * (which is {@code shortCircuitAggressively == true} is such that the whole expression isConstant if <em>any</em>
     * of its inputs is a const NULL. This is contrary to the standard behavior of composed expressions, which
     * is that the expression isConstant only if <em>all</em> of its inputs are</p>
     * <p>That's fine, except that it causes all of ComposedExpressionTestBase's tests to fail. Those tests give us
     * other useful tests, so rather than throw them out, we can turn our isConstant computation lazy. If this argument
     * is {@code false}, the expression will compute isConstant in the usual fashion.</p>
     * @param children the inputs
     * @param shortCircuitAggressively if true (default behavior), any const NULL will cause this expression to
     * become a const NULL.
     */
    ConcatExpression(List<? extends Expression> children, boolean shortCircuitAggressively) {
        super(AkType.VARCHAR, children);
        this.shortCircuitLazily = ! shortCircuitAggressively;
    }

    public ConcatExpression(List<? extends Expression> children) {
        this(children, true);
    }

    private final boolean shortCircuitLazily;

    private static final class InnerEvaluation extends AbstractCompositeExpressionEvaluation {
        @Override
        public ValueSource eval() {
            StringBuilder sb = new StringBuilder();
            AkibanAppender appender = AkibanAppender.of(sb);
            for (ExpressionEvaluation childEvaluation : children()) {
                ValueSource childSource = childEvaluation.eval();
                if (childSource.isNull())
                    return NullValueSource.only();
                childSource.appendAsString(appender, Quote.NONE);
            }
            return new ValueHolder(AkType.VARCHAR, sb.toString());
        }

        private InnerEvaluation(List<? extends ExpressionEvaluation> children) {
            super(children);
        }
    }
}
