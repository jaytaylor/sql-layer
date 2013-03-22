
package com.akiban.server.expression.std;

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.expression.std.Matchers.Index;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import java.util.List;

public class SubstringIndexExpression extends AbstractTernaryExpression
{
    @Scalar("substring_index")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer()
    {
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 3)
                throw new WrongExpressionArityException(3, argumentTypes.size());
            
            argumentTypes.setType(0, AkType.VARCHAR);
            argumentTypes.setType(1, AkType.VARCHAR);
            argumentTypes.setType(2, AkType.LONG);
            return argumentTypes.get(0);
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new SubstringIndexExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.RETURN_NULL;
        }
        
    };

    @Override
    public String name() {
        return "SUBSTRING_INDEX";
    }
    
    private static class InnerEvaluation extends AbstractThreeArgExpressionEvaluation
    {
        private Matcher matcher = null;        
        private String oldSubstr;
        
        InnerEvaluation (List<? extends ExpressionEvaluation> evals)
        {
            super(evals);
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource strSource;
            ValueSource substrSource;
            ValueSource countSource;
            
            if ((strSource = first()).isNull() 
                    || (substrSource = second()).isNull()
                    || (countSource = third()).isNull())
                return NullValueSource.only();
            
            String str = strSource.getString();
            String substr = substrSource.getString();
            int count = (int)countSource.getLong();
            boolean signed;
                        
            if (count == 0 || str.isEmpty() || substr.isEmpty())
            {
                valueHolder().putString("");
                return valueHolder();
            }
            else if (signed = count < 0)
            {
                count = -count;
                str = new StringBuilder(str).reverse().toString();
                substr = new StringBuilder(substr).reverse().toString();
            }

            // try to reuse compiled pattern if possible
            if (matcher == null || !substr.equals(oldSubstr))
            {
                oldSubstr = substr;
                matcher = new Index(substr);
            }
            
            int index = matcher.match(str, count);

            
            String ret = index < 0 // no match found
                    ? str 
                    : str.substring(0, index);
            if (signed)
                ret = new StringBuilder(ret).reverse().toString();
            valueHolder().putString(ret);
            return valueHolder();
        }
        
    }
    
    SubstringIndexExpression (List<? extends Expression> args)
    {
        super(AkType.VARCHAR, args);
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(name());
    }

    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }
}
