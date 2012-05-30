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

import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.*;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import java.util.HashMap;
import java.util.List;

public class PeriodAddExpression extends AbstractBinaryExpression {
    @Scalar("period_add")
    public static final ExpressionComposer COMPOSER = new InternalComposer();
    
    private static class InternalComposer extends BinaryComposer
    {
        
        @Override
        protected Expression compose(Expression first, Expression second)
        {
            return new PeriodAddExpression(first, second);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            int argc = argumentTypes.size();
            if (argc != 2)
                throw new WrongExpressionArityException(2, argc);
            
            for (int i = 0; i < argc; i++)
                argumentTypes.setType(i, AkType.LONG);
            
            // Return YYYYMM
            return ExpressionTypes.LONG;
        }
    }
    
    
    private static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
                
        public InnerEvaluation(List<? extends ExpressionEvaluation> childrenEval) 
        {
            super(childrenEval);
        }
        
        @Override
        public ValueSource eval()
        {
            if (left().isNull() || right().isNull())
                return NullValueSource.only();
            
            long period = left().getLong();
            // Compute actual year based on length of input

            long originalMonth = period % 100;
            long originalYear = parseYearFromPeriod(period);
            long offsetMonths = right().getLong();
            
            long totalMonths = (originalYear * 12 + originalMonth - 1) + offsetMonths;
            long result = createPeriod(totalMonths / 12, (totalMonths % 12) + 1);
                    
            valueHolder().putLong(result);
            return valueHolder();
        }
        
    }

    // Takes a period and adds the current millenium to it if the period is in YYMM or YMM format
    protected static Long parseYearFromPeriod(Long period)
    {
        final long CURRENT_MILLENIUM = 2000;
        long rawYear = period / 100;
        if (period <= 9999)
            rawYear += CURRENT_MILLENIUM;       
        return rawYear;
    }
    // Create a YYYYMM format from a year and month argument
    private static Long createPeriod(Long year, Long month) {
        return Long.valueOf(String.format("%d", year)
                + String.format("%02d", month));
    }
    
    // End helper functions ***************************************************
    
    protected PeriodAddExpression(Expression leftOperand, Expression rightOperand)
    {
        super(AkType.LONG, leftOperand, rightOperand);
    }
    
    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("PERIOD_ADD");
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
