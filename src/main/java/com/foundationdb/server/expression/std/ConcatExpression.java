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
import com.foundationdb.server.Quote;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.explain.Type;
import com.foundationdb.server.explain.std.ExpressionExplainer;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.expression.TypesList;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.sql.StandardException;
import com.foundationdb.util.AkibanAppender;
import java.math.BigDecimal;

import java.util.List;
import java.util.Map;

public final class ConcatExpression extends AbstractCompositeExpression {

    static class ConcatComposer implements ExpressionComposer {

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

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new ConcatExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.RETURN_NULL;
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
    public String name () {
        return "CONCATENATE";
    }
    
    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = super.getExplainer(context);
        ex.addAttribute(Label.INFIX_REPRESENTATION, PrimitiveExplainer.getInstance("||"));
        ex.addAttribute(Label.ASSOCIATIVE, PrimitiveExplainer.getInstance(true));
        return ex;
    }
    
    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(childrenEvaluations());
    }

    @Override
    public boolean nullIsContaminating() {
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
