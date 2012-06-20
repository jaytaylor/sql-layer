/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.expression.std;

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.error.InvalidCharToNumException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.sql.StandardException;
import com.akiban.server.expression.TypesList;

public class InetatonExpression extends AbstractUnaryExpression
{   
    @Scalar ("inet_aton")
    public static final ExpressionComposer COMPOSER = new UnaryComposer ()
    {
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            return new InetatonExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            argumentTypes.setType(0, AkType.VARCHAR);
            return ExpressionTypes.LONG;
        }
    };

    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private static final long FACTORS[] = {16777216L,  65536, 256};        
        public InnerEvaluation (ExpressionEvaluation ev)
        {
            super(ev);
        }

        @Override
        public ValueSource eval()
        {
            String strs[];
            if ((strs = Extractors.getStringExtractor().getObject(operand()).split("\\.")).length > 4)
                return NullValueSource.only();
            try
            {
                short num = Short.parseShort(strs[strs.length-1]);                
                long sum = num;
                if (sum < 0 || sum > 255) return NullValueSource.only();
                else if(strs.length == 1) return new ValueHolder(AkType.LONG, sum);                
                for (int i = 0; i < strs.length -1; ++i)
                {                    
                    if ((num = Short.parseShort(strs[i])) < 0 || num > 255) return NullValueSource.only();
                    sum += num * FACTORS[i];
                }
                valueHolder().putLong(sum);
                return valueHolder();
            }
            catch (NumberFormatException e)
            {
                QueryContext context = queryContext();
                if (context != null) 
                    context.warnClient(new InvalidCharToNumException(e.getMessage()));
                return NullValueSource.only();
            }
        }
    }

    public InetatonExpression (Expression e)
    {
        super(AkType.LONG, e);
    }

    @Override
    public String name()
    {
        return "INET_ATON";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation());
    }
}
