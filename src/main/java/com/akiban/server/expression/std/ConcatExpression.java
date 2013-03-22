
package com.akiban.server.expression.std;

import com.akiban.qp.exec.Plannable;
import com.akiban.server.Quote;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.Label;
import com.akiban.server.explain.PrimitiveExplainer;
import com.akiban.server.explain.Type;
import com.akiban.server.explain.std.ExpressionExplainer;
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
import com.akiban.util.AkibanAppender;
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
