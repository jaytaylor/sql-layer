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
import com.akiban.server.types.extract.Extractors;
import com.akiban.sql.StandardException;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class ConvertTZExpression extends AbstractTernaryExpression
{
    @Scalar("convert_tz")
    public static ExpressionComposer COMPOSER = new TernaryComposer()
    {
        @Override
        protected Expression doCompose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new ConvertTZExpression(arguments);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 3)
                throw new WrongExpressionArityException(3, 2);
            
            argumentTypes.setType(0, AkType.DATETIME);
            argumentTypes.setType(1, AkType.VARCHAR);
            argumentTypes.setType(2, AkType.VARCHAR);
            
            return ExpressionTypes.DATETIME;
        }
        
    };

    private static class InnerEvaluation extends AbstractThreeArgExpressionEvaluation
    {
        public InnerEvaluation(List<? extends ExpressionEvaluation> args)
        {
            super(args);
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource dt, from, to;
            
            if ((dt = first()).isNull()
                    || (from = second()).isNull()
                    || (to = third()).isNull())
                return NullValueSource.only();

            if (dt.getDateTime() / 1000000 == 0L) // zero date
                return NullValueSource.only();

            long ymd[] = Extractors.getLongExtractor(AkType.DATETIME).getYearMonthDayHourMinuteSecond(dt.getDateTime());

            String fromTz = from.getString();
            String toTz = to.getString();

            try
            {
                DateTime date = new DateTime((int)ymd[0], (int)ymd[1], (int)ymd[2],
                                             (int)ymd[3], (int)ymd[4], (int)ymd[5], 0,
                                             DateTimeZone.forID(fromTz));
                
                valueHolder().putDateTime(date.withZone(DateTimeZone.forID(toTz)));
            }
            catch (IllegalArgumentException e)
            {
                return NullValueSource.only();
            }
            return valueHolder();
        }
    }

    protected ConvertTZExpression(List<? extends Expression> args)
    {
        super(AkType.DATETIME, args);
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("CONVERT_TZ");
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

    @Override
    public String name()
    {
        return "CONVERT_TZ";
    }
    
}
