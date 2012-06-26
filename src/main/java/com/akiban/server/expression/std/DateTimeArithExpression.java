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
import com.akiban.server.expression.std.ArithOps.ArithOpComposer;
import com.akiban.server.expression.std.NumericToIntervalMillis.TargetType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.sql.StandardException;

public class DateTimeArithExpression extends ArithExpression
{
    @Scalar({"adddate", "date_add"})
    public static final ExpressionComposer ADD_DATE_COMPOSER = new AddSubComposer(ArithOps.ADD, TargetType.DAY);    
    
    @Scalar({"addtime"})
    public static final ExpressionComposer ADD_TIME_COMPOSER = new AddSubComposer(ArithOps.ADD, TargetType.SECOND);  
    
    @Scalar({"subdate", "date_sub"})
    public static final ExpressionComposer SUB_DATE_COMPOSER = new AddSubComposer(ArithOps.MINUS,  TargetType.DAY);
    
    @Scalar("subtime")
    public static final ExpressionComposer SUB_TIME_COMPOSER = new AddSubComposer(ArithOps.MINUS,  TargetType.SECOND);
    
    @Scalar("timediff")
    public static final ExpressionComposer TIMEDIFF_COMPOSER = new DiffComposer(AkType.TIME)
    {
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 2)
                throw new WrongExpressionArityException(2, argumentTypes.size());
            
            // if both arguments have UNSUPPORTED type, there's not much
            // that the type-inferring function can do other than
            // expecting them to be AkType.TIME 
            // since that's the type TIMEDIFF would "naturally" expect
            if (argumentTypes.get(0).getType() == AkType.UNSUPPORTED &&
                    argumentTypes.get(1).getType() == AkType.UNSUPPORTED)
            {
                argumentTypes.setType(0, AkType.TIME);
                argumentTypes.setType(1, AkType.TIME);
            }
            else
            {
                adjustType(argumentTypes, 0);
                adjustType(argumentTypes, 1);
            }
            return ExpressionTypes.TIME;
        }

        protected void adjustType (TypesList argumentTypes, int index) throws StandardException
        {
            ExpressionType dateType = argumentTypes.get(index);
            switch (dateType.getType())
            {
                case DATE:
                case TIME:
                case DATETIME:
                case TIMESTAMP: break;
                case VARCHAR:   argumentTypes.setType(index, dateType.getPrecision() > 10 ?
                                                     AkType.DATETIME: AkType.TIME);
                                break;
                                  // if the arg at this index is UNKNOWN (from params)
                                  // cast its type to the same type as the other's,
                                  // since the expression would expect two args to have
                                  // the same type
                case UNSUPPORTED: argumentTypes.setType(index, argumentTypes.get(1 - index).getType());
                                  break;                        
                default:        argumentTypes.setType(index, AkType.TIME);
            }
        }
    };

    @Scalar("datediff")
    public static final ExpressionComposer DATEDIFF_COMPOSER = new DiffComposer(AkType.LONG)
    {
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 2)
                throw new WrongExpressionArityException(2, argumentTypes.size());
            for (int n = 0; n < 2; ++n)
                argumentTypes.setType(n, AkType.DATE);

            return ExpressionTypes.LONG;
        }
    };

    private static class AddSubComposer extends BinaryComposer
    {
        private final ArithOpComposer composer;
        private final TargetType type;
        protected AddSubComposer (ArithOpComposer composer, TargetType type)
        {
            this.composer = composer;
            this.type = type;
        }
        @Override
        protected Expression compose(Expression first, Expression second, ExpressionType firstType, ExpressionType secondType, ExpressionType resultType)
        {
            if (ArithExpression.isNumeric(second.valueType()))
                second = new NumericToIntervalMillis(second, type);

            return composer.compose(first, second);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
             if (argumentTypes.size() != 2) throw new WrongExpressionArityException(2, argumentTypes.size());

            AkType firstArg = argumentTypes.get(0).getType();
            AkType secondArg = argumentTypes.get(1).getType();

            if (firstArg == AkType.VARCHAR)            
                firstArg = argumentTypes.get(0).getPrecision() > 10 ?
                           AkType.DATETIME : type.operandType;    
            else if (firstArg == AkType.UNSUPPORTED)
                firstArg = type.operandType;

            if (firstArg == AkType.DATE
                    && (secondArg == AkType.INTERVAL_MILLIS || !isIntegral(secondArg)))
                firstArg = AkType.DATETIME;

            // adjust first arg
            argumentTypes.setType(0, firstArg);

            // second arg does not need *real* adjusting since
            //  - if it's a numeric type, it'll be *casted* to an interval_millis in compose()
            //  - if it's an interval , => expected
            //  - if it's anything else, then InvalidArgumentType will be thrown
            if (ArithExpression.isNumeric(secondArg))
                argumentTypes.set(1, ExpressionTypes.INTERVAL_MILLIS);

            return composer.composeType(argumentTypes);
        }        
        
        protected static boolean isIntegral (AkType type)
        {
            switch (type)
            {
                case DOUBLE:
                case DECIMAL:  return false;
                case LONG:
                case INT:
                case U_BIGINT:  
                default:        return true;
            }
        }
    }

    private abstract static class DiffComposer extends BinaryComposer
    {
        private final AkType topT;
        public DiffComposer (AkType topT)
        {
            this.topT = topT;
        }

        @Override
        protected Expression compose(Expression first, Expression second, ExpressionType firstType, ExpressionType secondType, ExpressionType resultType)
        {
            return new DateTimeArithExpression(first, second, topT);
        }
    }

    protected static final class Calculator
    {
        private static final long M_SECS_OF_DAY = 86400000L;

        /**
         *
         * @param interval: in millisecs
         * @return interval in day
         *
         */
        public static long getDay (long interval)
        {
            return interval / M_SECS_OF_DAY;
        }

        /**
         *
         * @param interval: a positive number representing an interval between
         *                  two events in millisecs
         * @return interval in H,M,S
         */
        public static long[] getHMS (long interval)
        {
            long seconds = interval / 1000L;
            long hours = seconds / 3600;
            long minutes = (seconds - hours * 3600) / 60;
            seconds -= (minutes * 60 + hours * 3600);
            return new long[] {hours, minutes, seconds};
        }
    }
    
    protected static class InnerValueSource extends ArithExpression.InnerValueSource
    {
        public InnerValueSource (ArithOp op, AkType topT)
        {
            super(op, topT);
        }

        /**
         *
         * @return number of DAY between two events
         */
        @Override
        public long getLong ()
        {
            return Calculator.getDay(rawInterval());
        }

        /**
         *
         * @return INTERVAL_MILLIS between two events expressed in HOUR:MINUTE:SECOND
         */
        @Override
        public long getTime ()
        {
            check(AkType.TIME);
            long millis = rawInterval();
            long sign;
            if (millis < 0)
                millis *= (sign = -1);
            else
                sign = 1;
            long hms[] = Calculator.getHMS(millis);
            return sign * (hms[0] * 10000L + hms[1] * 100 + hms[2]);
        }
    }

    /**
     * 
     * @param left
     * @param right
     * @param topT expected top type
     * 
     * topT is explicitly set. 
     * This type will take precedence over ArithExpression's getTopType()'s decision
     */
    protected DateTimeArithExpression (Expression left, Expression right, AkType topT)
    {
        super(left, ArithOps.MINUS, right, topT);        
    }
    
    @Override
    protected InnerValueSource getValueSource (ArithOp op)
    {
        return new InnerValueSource(op, topT);
    }

    @Override
    public ExpressionEvaluation evaluation ()
    {
        return new InnerEvaluation(op, this, childrenEvaluations(), top);
    }    
}
