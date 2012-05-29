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
            
            // Period as VARCHAR for easier handling of leading zeroes
            argumentTypes.setType(0, AkType.VARCHAR);
            argumentTypes.setType(1, AkType.LONG);
            
            // Return YYYYMM, 6 characters
            return ExpressionTypes.varchar(6);
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
            
            // Parse the input strings for a dictionary with "year" and "month" as keys
            HashMap<String, Long> parsedLeftMap = parsePeriod(left().getString());
            long originalYear = parsedLeftMap.get("year");
            long originalMonth = parsedLeftMap.get("month");
            
            long offsetMonths = right().getLong();
            
            
            // Calculate the new period with left-padded years
            // Subtract 1 from total month count, converting months from 1-12 -> 0-11
            long totalMonths = (originalYear * 12 + originalMonth - 1) + offsetMonths;
            valueHolder().putString(createPeriod(totalMonths / 12, (totalMonths % 12) + 1));
            return valueHolder();
        }
        
    }
    
    // Begin helper functions *************************************************
    // Periods have the format of either YYMM or YYYYMM
    protected static HashMap<String, Long> parsePeriod(String periodAsStr) {
        // Check format of period through length
        long periodLen = periodAsStr.length();
        if (periodLen != 4 && periodLen != 6) {
            throw new InvalidParameterValueException(
                    String.format(
                    "Invalid format for period %s (must be YYMM or YYYYMM)", 
                    periodAsStr));
        }

        // Check format of period for non-numerical characters
        long periodAsLong = -1;
        try {
            periodAsLong = Long.parseLong(periodAsStr);
        } catch (Exception e) {
            throw new InvalidParameterValueException(
                    String.format("Period %s contains non-numerical characters.", 
                    periodAsStr));
        }

        long CURRENT_MILLENIUM = 2000;
        long parsedYear = (periodLen == 4)
                ? CURRENT_MILLENIUM + (periodAsLong / 100) : (periodAsLong / 100);
        long parsedMonth = periodAsLong % 100;

        // Make sure that only valid months are entered
        if (parsedMonth < 1 || parsedMonth > 12) {
            throw new InvalidParameterValueException(
                    String.format("Month %d of period %s out of range.", 
                    parsedMonth, periodAsStr));
        }
   
        HashMap<String, Long> results = new HashMap<String, Long>();
        results.put("year", parsedYear);
        results.put("month", parsedMonth);

        return results;
    }

    // Create a YYYYMM format from a year and month argument
    private static String createPeriod(Long year, Long month) {
        return String.format("%04d", year)
                + String.format("%02d", month);
    }
    
    // End helper functions ***************************************************
    
    protected PeriodAddExpression(Expression leftOperand, Expression rightOperand)
    {
        super(AkType.VARCHAR, leftOperand, rightOperand);
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
