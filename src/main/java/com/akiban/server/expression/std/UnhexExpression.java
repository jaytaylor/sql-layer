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
import java.util.HashSet;
import java.util.Set;

public class UnhexExpression extends AbstractUnaryExpression
{
    @Scalar("unhex")
    public static final ExpressionComposer COMPOSER = new UnaryComposer()
    {
        @Override
        protected Expression compose(Expression argument)
        {
            return new UnhexExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            
            argumentTypes.setType(0, AkType.VARCHAR);
            return ExpressionTypes.varchar(argumentTypes.get(0).getPrecision() / 2 + 1);
        }
        
    };
    
    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private static final int BASE_CHAR = 10 -'a';
        private static final Set<Character> LEGAL = new HashSet<Character>();
        static
        {
            for (char ch = 'a'; ch <= 'f'; ++ch)
                LEGAL.add(ch);
            for (char ch = '0'; ch <= '9'; ++ch)
                LEGAL.add(ch);
        }
        
        InnerEvaluation(ExpressionEvaluation arg)
        {
            super(arg);
        }
        //b df ee re
        @Override
        public ValueSource eval()
        {
            ValueSource source = operand();
            if (source.isNull())
                return NullValueSource.only();
            
            String st = source.getString();
            if (st.isEmpty())
                valueHolder().putString(st);
            else
            {
                StringBuilder out = new StringBuilder();
                char c1, c2;
                int start = 1, end;
                   
                // check if the first digit is a 'legal' hex 
                if (!LEGAL.contains(c1 = (char)(st.charAt(0) | 32)))
                        return NullValueSource.only();
                
                // check to see if the hex string can be evenly divided
                // into pairs
                if (st.length() % 2 == 0)
                {
                    if (!LEGAL.contains(c2 = (char)(st.charAt(1) | 32)))
                        return NullValueSource.only();

                    out.append((char)((c1 > 'a' ? c1 + BASE_CHAR : c1 - '0') * 16
                            + (c2 > 'a' ? c2 + BASE_CHAR : c2 - '0')));
                    ++start;
                   
                }
                else
                    out.append((char)(c1 >= 'a' ? c1 + BASE_CHAR : c1 - '0')); 
                
                end = st.length() -1;
                for (; start < end; ++start)
                {
                    if (!LEGAL.contains(c1 = (char)(st.charAt(start) | 32)))
                        return NullValueSource.only();
                    
                    if (!LEGAL.contains(c2 = (char)(st.charAt(++start) | 32)))
                        return NullValueSource.only();
                    
                    
                    out.append((char)((c1 > 'a' ? c1 + BASE_CHAR : c1 - '0') * 16
                            + (c2 > 'a' ? c2 + BASE_CHAR : c2 - '0')));
                }
                
                valueHolder().putString(out.toString());
            }
            
            return valueHolder();
        }
    }
    
    UnhexExpression (Expression arg)
    {
        super(AkType.VARCHAR, arg);
    }
    
    @Override
    protected String name()
    {
        return "UNHEX";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation());
    }
    
}
