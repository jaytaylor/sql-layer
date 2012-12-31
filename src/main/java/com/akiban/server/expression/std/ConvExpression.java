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
import com.akiban.server.types.util.ValueHolder;
import com.akiban.sql.StandardException;
import java.math.BigInteger;
import java.util.List;

public class ConvExpression extends AbstractTernaryExpression
{
    private static final int MAX_BASE = 36;
    private static final int MIN_BASE = 2;
    
    @Scalar("conv")
    public static final ExpressionComposer COMPOSER = new InternalComposer();

    @Override
    public String name() {
        return "CONV";
    }
    
    private static class InternalComposer extends TernaryComposer
    {
        // TODO: change the hard coded 2 and 36 to the actual values
        // of FROM_BASE and TO_BASE
        // It is not possible yet to get these values at this point.
        //
        // But this current value of FACTOR is the largest possible expansion
        // of a string converted from base x to base y, where x,y ∈ [2, 36]
        private static final double FACTOR = Math.log(MAX_BASE) / Math.log(MIN_BASE);
        
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 3)
                throw new WrongExpressionArityException(3, argumentTypes.size());
            
            // NUM argument (should be string)
            argumentTypes.setType(0, AkType.VARCHAR);
            
            // FROM_BASE argument
            argumentTypes.setType(1, AkType.LONG);
            
            // TO_BASE argument
            argumentTypes.setType(2, AkType.LONG);
            
            return ExpressionTypes.varchar((int)Math.round(argumentTypes.get(0).getPrecision() * FACTOR));
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new ConvExpression(arguments);
        }

        @Override
        protected Expression doCompose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new ConvExpression(arguments);
        }
    };
    
    private static class InnerEvaluation extends AbstractThreeArgExpressionEvaluation
    {
        private static final ValueSource ZERO = new ValueHolder(AkType.VARCHAR, "0");
        private static final BigInteger N64 = new BigInteger("FFFFFFFFFFFFFFFF", 16);
        
        InnerEvaluation(List<? extends ExpressionEvaluation> evals)
        {
            super(evals);
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource num = first();
            ValueSource from = second();
            ValueSource to = third();
            
            int fromBase;
            int toBase;
            
            if (num.isNull() || from.isNull() || to.isNull()
                    || !isInRange(fromBase = (int)from.getLong(), MIN_BASE, MAX_BASE)
                    || !isInRange(Math.abs(toBase = (int)to.getLong()), MIN_BASE, MAX_BASE)) // toBase can be negative
                return NullValueSource.only();
            
            try
            {
                valueHolder().putString(doConvert(
                        truncateFactional(num.getString()), 
                        fromBase, 
                        toBase));
                return valueHolder();
            }
            catch (NumberFormatException e) // invalid digits input will
            {                               // result in ZERO string (as per MySQL)
                return ZERO;
            }
        }
        
        private static boolean isInRange(int num, int min, int max)
        {
            return num <= max && num >= min;
        }
        
        /**
         * 
         * @param st: a numeric string
         * @return the substring starting from [0,n-1], where n is the position
         *         of the first '.'
         */
        private static String truncateFactional (String st)
        {
            int dot = st.indexOf('.');
            if (dot >= 0)
                return st.substring(0, dot);
            else
                return st;
        }
    
        /**
         * 
         * @param st: numeric string
         * @return a string representing the value in st in toBase.
         * 
         * if toBase is unsigned, the value contained in st would
         * be interpreted as an unsigned value
         * (Thus, -1 would be the same as FFFFFFFFFFFFFFFF)
         */
        private static String doConvert (String st, int fromBase, int toBase)
        {
            
            boolean signed = toBase < 0;
            if (signed)
                toBase = -toBase;
            
            BigInteger num = new BigInteger (st, fromBase);
            
            // if the number is signed and the toBase value is unsigned
            // interpret the number as unsigned
            if (!signed && num.signum() < 0)
                num = num.abs().xor(N64).add(BigInteger.ONE);
            
            return num.toString(toBase).toUpperCase();
        }
    }
    
    ConvExpression(List<? extends Expression> args)
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
