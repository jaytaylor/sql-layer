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
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.sql.StandardException;
import com.akiban.sql.optimizer.ArgList;

import java.util.List;

public final class CoalesceExpression extends AbstractCompositeExpression {

    @Scalar("coalesce")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer() {
        @Override
        public Expression compose(List<? extends Expression> arguments) {
            return new CoalesceExpression(arguments);
        }

        @Override
        public ExpressionType composeType(ArgList argumentTypes) throws StandardException
        {
            AkType akType = AkType.NULL;
            for (int i = 0; i < argumentTypes.size(); i++)
                if (akType != AkType.NULL)
                    argumentTypes.setArgType(i, akType);
                else
                    akType = argumentTypes.get(i).getType();


            for (ExpressionType type : argumentTypes) 
                if (type.getType() != AkType.NULL)
                    return type;
            
            return ExpressionTypes.NULL;
        }
    };

    @Override
    protected void describe(StringBuilder sb) {
        sb.append("COALESCE");
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(valueType(), childrenEvaluations());
    }

    @Override
    protected boolean nullIsContaminating() {
        return false;
    }

    public CoalesceExpression(List<? extends Expression> children) {
        super(childrenType(children), children);
    }

    private static final class InnerEvaluation extends AbstractCompositeExpressionEvaluation {

        @Override
        public ValueSource eval() {
            for (ExpressionEvaluation childEvaluation : children()) {
                ValueSource childSource = childEvaluation.eval();
                if (!childSource.isNull()) {
                    return type.equals(childSource.getConversionType())
                            ? childSource
                            : Converters.convert(childSource, holder());
                }
            }
            return NullValueSource.only();
        }

        private InnerEvaluation(AkType type, List<? extends ExpressionEvaluation> children) {
            super(children);
            this.type = type;
        }

        private ValueHolder holder() {
            valueHolder().expectType(type);
            return valueHolder();
        }

        private final AkType type;        
    }
    }
