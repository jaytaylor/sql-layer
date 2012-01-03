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
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.BoolValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.sql.StandardException;
import com.akiban.server.expression.TypesList;
import com.akiban.util.ArgumentValidation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class InExpression extends AbstractCompositeExpression {

    @Scalar("in")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer() {
        @Override
        public Expression compose(List<? extends Expression> arguments) {
            return new InExpression(arguments.get(0), arguments.subList(0, arguments.size()));
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            AkType firstArg = argumentTypes.get(0).getType();
            for (int i = 1; i < argumentTypes.size(); ++i)
                argumentTypes.setType(i, firstArg);
            return ExpressionTypes.BOOL;
        }
    };

    @Override
    protected void describe(StringBuilder sb) {
        sb.append("IN");
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(childrenEvaluations());
    }

    @Override
    protected boolean nullIsContaminating() {
        return false;
    }

    public InExpression(Expression lhs, List<? extends Expression> rhs) {
        super(AkType.BOOL, combine(lhs, rhs));
        if (rhs.isEmpty())
            throw new IllegalArgumentException("rhs cannot be empty");
    }

    private static List<? extends Expression> combine(Expression head, List<? extends Expression> tail) {
        List<Expression> list = new ArrayList<Expression>();
        list.add(head);
        list.addAll(tail);
        return list;
    }

    private static final class InnerEvaluation extends AbstractCompositeExpressionEvaluation {

        @Override
        public ValueSource eval() {
            Iterator<? extends ExpressionEvaluation> childEvals = children().iterator();
            assert childEvals.hasNext() : "must have at least 1 child";
            ValueSource lhsSource = childEvals.next().eval();
            if (lhsSource.isNull())
                return BoolValueSource.of(null);
            lhsScratch.copyFrom(lhsSource);
            boolean foundNull = false;
            do {
                ValueSource rhsSourceRaw = childEvals.next().eval();
                if (rhsSourceRaw.isNull()) {
                    foundNull = true;
                }
                else {
                    ValueHolder rhsSource = copyAs(rhsSourceRaw, lhsSource.getConversionType());
                    if (lhsScratch.equals(rhsSource))
                        return BoolValueSource.of(true);
                }
            } while (childEvals.hasNext());

            return foundNull ? BoolValueSource.of(null) : BoolValueSource.of(false);
        }

        private InnerEvaluation(List<? extends ExpressionEvaluation> children) {
            super(children);
            ArgumentValidation.isGTE("number of children", children.size(), 2);
        }

        private ValueHolder copyAs(ValueSource source, AkType as) {
            switch (as.underlyingType()) {
            case BOOLEAN_AKTYPE:
                rhsScratch.expectType(AkType.BOOL);
                rhsScratch.putBool(Extractors.getBooleanExtractor().getBoolean(source, null));
                break;
            case LONG_AKTYPE:
                rhsScratch.putRaw(as, Extractors.getLongExtractor(as).getLong(source));
                break;
            case FLOAT_AKTYPE:
                rhsScratch.putRaw(as, (float)Extractors.getDoubleExtractor().getDouble(source));
                break;
            case DOUBLE_AKTYPE:
                rhsScratch.putRaw(as, Extractors.getDoubleExtractor().getDouble(source));
                break;
            case OBJECT_AKTYPE:
                rhsScratch.putRaw(as, Extractors.getObjectExtractor(as).getObject(source));
                break;
            default: throw new AssertionError("unknown underlying type: " + as);
            }
            return rhsScratch;
        }

        private final ValueHolder lhsScratch = new ValueHolder();
        private final ValueHolder rhsScratch = new ValueHolder();
    }
}
