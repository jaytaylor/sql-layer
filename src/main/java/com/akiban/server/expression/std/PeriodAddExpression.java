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
import com.akiban.server.expression.*;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import java.util.List;

public class PeriodAddExpression extends AbstractBinaryExpression {
    @Scalar("period_add")
    public static final ExpressionComposer COMPOSER = new InternalComposer();

    @Override
    public String name() {
        return "PERIOD_ADD";
    }
    
    private static class InternalComposer extends BinaryComposer
    {
        
        @Override
        protected Expression compose(Expression first, Expression second, ExpressionType firstType, ExpressionType secondType, ExpressionType resultType)
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
            long offsetMonths = right().getLong();
            
            // COMPATIBILITY: MySQL currently has undefined behavior for negative numbers
            // Our behavior follows our B.C. year numbering (-199402 + 1 = -199401)
            // Java's mod returns negative numbers: -1994 % 100 = -94
            long periodInMonths = fromPeriod(period);
            long totalMonths = periodInMonths + offsetMonths;
            
            // Handle the case where the period changes sign (e.g. -YYMM to YYMM)
            // as a result of adding. Since 0000 months is not really a date,
            // this leads to an off by one error
            if (Long.signum(periodInMonths) * Long.signum(totalMonths) == -1)
                totalMonths -= Long.signum(totalMonths) * 2;
            
            long result = toPeriod(totalMonths);
            valueHolder().putLong(result);
            return valueHolder();
        }
        
    }

    // Helper functions
    // Takes a period and returns the number of months from year 0
    protected static long fromPeriod(long period)
    {
        long periodSign = Long.signum(period);
        
        long rawMonth = period % 100;
        long rawYear = period / 100;

        long absValYear = Math.abs(rawYear);
        if (absValYear < 70)
            rawYear += periodSign * 2000;
        else if (absValYear < 100)
            rawYear += periodSign * 1900; 
        
        return (rawYear * 12 + rawMonth - (1 * periodSign));
    }
    
    // Create a YYYYMM format from a number of months
    protected static long toPeriod(long monthCount) {
        long year = monthCount / 12;
        long month = (monthCount % 12) + 1 * Long.signum(monthCount);
        return (year * 100) + month;
    }
    
    // End helper functions ***************************************************
    
    protected PeriodAddExpression(Expression leftOperand, Expression rightOperand)
    {
        super(AkType.LONG, leftOperand, rightOperand);
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
