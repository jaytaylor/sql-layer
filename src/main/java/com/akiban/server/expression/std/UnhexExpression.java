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
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.InvalidParameterValueException;
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
        
        /**
         * It is assumed that if c is a character, it's a lowercase one
         * 
         * @param c: character with this (ASCII) code
         * @return the HEX value of this char
         * 
         * Eg., 97 (or 'a') would return 10
         *      45 (or '0') would return 0
         */
        private static int getHexVal (int c)
        {
            return c > 'a'
                    ? c + BASE_CHAR
                    : c - '0';
        }
        
        /**
         * 
         * @param highChar
         * @param lowChar
         * @return a character whose (ASCII) code is eqal to the hexadecimal value 
         *          of <highChar><lowChar>
         * @throws InvalidParameterValue if either of the two char is not a legal
         *          hex digit
         * 
         * Eg., parseByte('2', '0')  should return ' ' (space character)
         * 
         */
        private static char parseByte(char highChar, char lowChar)
        {
            // convert all to lowercase
            highChar |= 32;
            lowChar |= 32;
            
            if (!LEGAL.contains(highChar) || !LEGAL.contains(lowChar))
                throw new InvalidParameterValueException("Invalid HEX digit(s): " 
                        + highChar + " " + lowChar);
            
            return (char)((getHexVal(highChar) << 4) + getHexVal(lowChar));
        }
        
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
                try
                {
                    StringBuilder out = new StringBuilder();
                    int start, end;

                    // check if the hex string can be evenly divided into pairs
                    // if so, the firt two digits will make a byte (a character)
                    if (st.length() % 2 == 0)
                    {
                        out.append(parseByte(st.charAt(0) ,st.charAt(1)));
                        start = 2;
                    }
                    else // if not, only the first char will
                    {
                        out.append(parseByte('0', st.charAt(0)));
                        start = 1;
                    }

                    // starting from here, all characters should be evenly divided into pairs
                    end = st.length() -1;
                    for (; start < end; ++start)
                        out.append(parseByte(st.charAt(start), st.charAt(++start)));                
                    valueHolder().putString(out.toString());
                }
                catch (InvalidOperationException e)
                {
                    QueryContext qc = queryContext();
                    if (qc != null)
                        qc.warnClient(e);
                    return NullValueSource.only();
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
