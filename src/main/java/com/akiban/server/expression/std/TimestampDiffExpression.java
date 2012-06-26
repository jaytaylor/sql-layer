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
import com.akiban.server.error.InvalidArgumentTypeException;
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
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.sql.StandardException;
import com.akiban.sql.parser.TernaryOperatorNode;
import java.util.List;
import org.joda.time.DateTimeZone;
import org.joda.time.IllegalFieldValueException;

public class TimestampDiffExpression extends AbstractTernaryExpression
{
    @Scalar("timestampDiff")
    public static final ExpressionComposer COMPOSER = new TernaryComposer()
    {
        @Override
        protected Expression doCompose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new TimestampDiffExpression(arguments);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 3)
                throw new WrongExpressionArityException(3, argumentTypes.size());

            return ExpressionTypes.LONG;
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new TimestampDiffExpression(arguments);
        }
    };

    @Override
    public String name() {
        return "TIMESTAMPDIFF";
    }
    
    private static class InnerEvaluation extends AbstractThreeArgExpressionEvaluation
    {
        private static final long[] MILLIS_DIV = new long[6];
        private static final long[] MONTH_DIV = {12L, 4L, 1L};
        
        private static final int MILLIS_BASE = TernaryOperatorNode.WEEK_INTERVAL;
        private static final int MONTH_BASE = TernaryOperatorNode.YEAR_INTERVAL;
        
        static
        {
            int mul[] = {7, 24, 60, 60, 1000};

            MILLIS_DIV[5] = 1;
            for (int n = 4; n >= 0; --n)
                MILLIS_DIV[n] = MILLIS_DIV[n + 1] * mul[n];
        }
        
        InnerEvaluation (List<? extends ExpressionEvaluation> evals)
        {
            super(evals);
        }
        
        @Override
        public ValueSource eval()
        {
            
            ValueSource intervalType = children().get(0).eval(); 
            ValueSource date1 = children().get(1).eval();
            ValueSource date2 = children().get(2).eval();
            
            if (intervalType.isNull() || date1.isNull() || date2.isNull())
                return NullValueSource.only();
            
            int type = (int)intervalType.getLong();
            try
            {
                switch(type)
                {
                    case TernaryOperatorNode.YEAR_INTERVAL:
                    case TernaryOperatorNode.QUARTER_INTERVAL:
                    case TernaryOperatorNode.MONTH_INTERVAL:
                        valueHolder().putLong(doSubstract(tryGetYMD(date2), tryGetYMD(date1))
                                / MONTH_DIV[type - MONTH_BASE]);
                        break;
                    case TernaryOperatorNode.WEEK_INTERVAL:
                    case TernaryOperatorNode.DAY_INTERVAL:
                    case TernaryOperatorNode.HOUR_INTERVAL:
                    case TernaryOperatorNode.MINUTE_INTERVAL:
                    case TernaryOperatorNode.SECOND_INTERVAL:
                    case TernaryOperatorNode.FRAC_SECOND_INTERVAL:
                        valueHolder().putLong((tryGetUnix(date2) - tryGetUnix(date1)) 
                                / MILLIS_DIV[type - MILLIS_BASE]);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unknown INTERVAL_TYPE: " + type);
                }
                return valueHolder();
            }
            catch (InvalidOperationException ex)
            {
                QueryContext qc = queryContext();
                if (qc != null)
                    qc.warnClient(ex);
                return NullValueSource.only();
            }
            catch (IllegalFieldValueException e)
            {
                QueryContext qc = queryContext();
                if (qc != null)
                    qc.warnClient(new InvalidParameterValueException(e.getMessage()));
                return NullValueSource.only();
            }
        }
        
        private static long doSubstract (long d1[], long d2[])
        {
            if (!ArithExpression.InnerValueSource.vallidDayMonth(d1[0], d1[1], d1[2])
                    || !ArithExpression.InnerValueSource.vallidDayMonth(d2[0], d2[1], d2[2]))
                throw new InvalidParameterValueException("Invalid date/time values");

            long ret = (d1[0] - d2[0]) * 12 + d1[1] - d2[1];
            
            // adjust the day difference
            if (ret > 0 && d1[2] < d2[2]) --ret;
            else if (ret < 0 && d1[2] > d2[2]) ++ret;
                            
            return ret;
        }
        
        private static long[] tryGetYMD(ValueSource source)
        {
            long val = 0;
            LongExtractor extractor = Extractors.getLongExtractor(AkType.DATE);
            AkType t = source.getConversionType();
            
            switch(t)
            {
                case DATE:      val = source.getDate(); break;
                case DATETIME:  val = source.getDateTime(); break;
                case TIMESTAMP: val = source.getTimestamp(); break;
                case VARCHAR:   val = extractor.getLong(source.getString());
                                t = AkType.DATE;
                                break;
                default:        throw new InvalidArgumentTypeException("Invalid Type for TIMESTAMPDIFF: " + t);
                    
            }
            return Extractors.getLongExtractor(t).getYearMonthDayHourMinuteSecond(val);
        }
        
        private static long tryGetUnix (ValueSource source)
        {
            AkType t = source.getConversionType();
            long val = 0;
            switch (t)
            {
                case DATE:      val = source.getDate(); break;
                case DATETIME:  val = source.getDateTime(); break;
                case TIMESTAMP: val = source.getTimestamp(); break;
                case VARCHAR:   String st = source.getString();
                                LongExtractor ext = Extractors
                                        .getLongExtractor(st.length() > 10 
                                                            ? AkType.DATETIME
                                                            : AkType.DATE);
                                return ext.stdLongToUnix(ext.getLong(st));
                default:        throw new InvalidArgumentTypeException("Unsupported type for TIMESTAMPDIFF: " + t);
                             
            }
            return Extractors.getLongExtractor(t).stdLongToUnix(val, DateTimeZone.UTC);
        }
    }
    
    TimestampDiffExpression (List<? extends Expression> args)
    {
        super(AkType.LONG, args);
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
