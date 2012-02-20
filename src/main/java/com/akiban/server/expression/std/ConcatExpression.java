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
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.Type;
import com.akiban.sql.optimizer.explain.std.ExpressionExplainer;
import com.akiban.util.AkibanAppender;

import java.util.List;

public final class ConcatExpression extends AbstractCompositeExpression {

    static class ConcatComposer implements ExpressionComposer {
        @Override
        public Expression compose(List<? extends Expression> arguments) {
            return new ConcatExpression(arguments);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            for (int i = 0; i < argumentTypes.size(); ++i)
                argumentTypes.setType(i, AkType.VARCHAR);

            int length = 0;
            for (ExpressionType type : argumentTypes) {
                switch (type.getType()) {
                case VARCHAR:
                    length += type.getPrecision();
                    break;
                case NULL:
                    return ExpressionTypes.NULL;
                default:
                    throw new AkibanInternalException("VARCHAR required, given " + type);
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
    public String name () {
        return "CONCATENATE";
    }
    
    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(childrenEvaluations());
    }

    @Override
    protected boolean nullIsContaminating() {
        return true;
    }

    /**
     * <p>Creates a CONCAT</p>
     * <p>CONCAT is NULL if any of its arguments are NULL, so the whole expression isConstant if <em>any</em>
     * of its inputs is a const NULL. </p>   
     * @param children the inputs    
     */
    ConcatExpression(List<? extends Expression> children){
        super(AkType.VARCHAR, children);
    }

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
            valueHolder().putString(sb.toString());
            return valueHolder();
        }

        private InnerEvaluation(List<? extends ExpressionEvaluation> children) {
            super(children);
        }
    }
}
