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

import com.akiban.server.error.WrongExpressionArityException;
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
import java.util.List;
import java.math.BigInteger;

public class ExportSetExpression extends AbstractCompositeExpression
{
    @Scalar("export_set")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer()
    {
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            switch(argumentTypes.size())
            {
                case 5:     argumentTypes.setType(4, AkType.LONG);     // fall thru
                case 4:     argumentTypes.setType(3, AkType.VARCHAR); // fall thru
                case 3:     argumentTypes.setType(2, AkType.VARCHAR);
                            argumentTypes.setType(1, AkType.VARCHAR);
                            argumentTypes.setType(0, AkType.U_BIGINT);
                            break;
                default:    throw new WrongExpressionArityException(3, argumentTypes.size());
            }
            
             return ExpressionTypes.newType(AkType.VARCHAR, 
                           64 + 63 * (  Math.max(   argumentTypes.get(2).getPrecision(), 
                                                    argumentTypes.get(1).getPrecision()) 
                                        + (argumentTypes.size() > 3 ? 
                                                    argumentTypes.get(3).getPrecision() : 
                                                    1)), 
                            0);
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            if (arguments.size() < 3 || arguments.size() > 5)
                throw new WrongExpressionArityException(3, arguments.size());
            return new ExportSetExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.RETURN_NULL;
        }
        
    };

    @Override
    public String name() {
        return "EXPORT_SET";
    }

    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        private static final BigInteger MASK = new BigInteger("ffffffffffffffff", 16);
        
        public InnerEvaluation (List<? extends ExpressionEvaluation> evals)
        {
            super(evals);
        }
        
        @Override
        public ValueSource eval()
        {
            for (ExpressionEvaluation child :children())
                if (child.eval().isNull())
                    return NullValueSource.only();

            BigInteger num = children().get(0).eval().getUBigInt().and(MASK);
            String bits[] = new String[]{children().get(2).eval().getString(),
                                         children().get(1).eval().getString()};
            String delim = ",";
            int len = 64; 
            
            switch(children().size())
            {
                case 5: len = Math.min((int)children().get(4).eval().getLong(), len); // fall thru
                case 4: delim = children().get(3).eval().getString();
            }
            
            StringBuilder builder = new StringBuilder();
            char digits[] = num.toString(2).toCharArray();
            
            // return value needs to be in little-endian format
            int count = 0;
            for (int n = digits.length - 1; n >= 0 && count < len; --n, ++count)
                builder.append(bits[digits[n]-'0']).append(delim);
            
            // fill the rest with 'off'
            for (; count < len; ++count)
                builder.append(bits[0]).append(delim);
            if (!delim.equals(""))
                builder.deleteCharAt(builder.length() -1);
            
            valueHolder().putString(builder.toString());
            return valueHolder();
        }
        
    }
    
    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(name());
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }
    
    private ExportSetExpression (List<? extends Expression> operands)
    {
        super(AkType.VARCHAR, operands);
    }
}
