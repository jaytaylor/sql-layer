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

import com.akiban.server.error.InvalidArgumentTypeException;
import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueSourceIsNullException;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.server.types.util.AbstractArithValueSource;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.Label;
import com.akiban.sql.optimizer.explain.PrimitiveExplainer;
import com.akiban.sql.optimizer.explain.Type;
import com.akiban.sql.optimizer.explain.std.ExpressionExplainer;
import com.akiban.util.ArgumentValidation;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class ArithExpression extends AbstractBinaryExpression
{
    protected final ArithOp op;
    protected AkType topT;
    protected ExpressionType top;
    
    /**
     * SUPPORTED_TYPES: contains all types that are supported in ArithExpression.
     *
     * Notes about the map's structure:
     * It is important that all date/time types be *put* with an even value key (e.g., 0, 2, 4, ... 2n)
     * and regular numeric types (double, int, etc) odd value key (e.g., 1, 3, 5, ... 2n +1),
     * since these keys are going to be used to determine the return type of the expression
     *
     * INTERVAL_MILLIS is put with '0' because INTERVAL_MILLIS is special; that is, it can "interact" with both date/time types
     * and regular numeric types
     */
    protected static final BidirectionalMap SUPPORTED_TYPES = new BidirectionalMap(10, 0.5f);
    private static final int HIGHEST_KEY;
    protected static final int DEFAULT_PRECISION = 20; 
    protected static final int DEFAULT_SCALE = 9;
    
    static
    {
        // date/time types : key is even
        SUPPORTED_TYPES.put(AkType.INTERVAL_MILLIS, 0);
        SUPPORTED_TYPES.put(AkType.DATE, 2);
        SUPPORTED_TYPES.put(AkType.TIME, 4);
        SUPPORTED_TYPES.put(AkType.DATETIME, 6);
        SUPPORTED_TYPES.put(AkType.YEAR, 8);
        SUPPORTED_TYPES.put(AkType.TIMESTAMP, 10);
        SUPPORTED_TYPES.put(AkType.INTERVAL_MONTH, HIGHEST_KEY = 12);

        // regular numeric types: key is odd
        SUPPORTED_TYPES.put(AkType.DECIMAL, 1);
        SUPPORTED_TYPES.put(AkType.DOUBLE, 3);
        SUPPORTED_TYPES.put(AkType.FLOAT, 5);
        SUPPORTED_TYPES.put(AkType.U_BIGINT, 7);
        SUPPORTED_TYPES.put(AkType.LONG, 9);
        SUPPORTED_TYPES.put(AkType.INT, 11);
    }
    
    public ArithExpression (Expression lhs, ArithOp op, Expression rhs, ExpressionType leftType, ExpressionType rightType, ExpressionType resultT)
    {
        super(getTopType(lhs.valueType(), rhs.valueType(), op), lhs, rhs);
        this.op = op;
        this.topT = super.valueType();
        if ((resultT != null) && (resultT.getType() == topT))
            top = resultT;
        else
            // Cases that don't match: 
            // * operation on INTs is INT, vs. LONG.
            // * tests don't supply expression types.
            top = ExpressionTypes.newType(topT, DEFAULT_PRECISION, DEFAULT_SCALE);
    }

    public ArithExpression (Expression lhs, ArithOp op, Expression rhs)
    {
        super(getTopType(lhs.valueType(), rhs.valueType(), op),lhs, rhs);
        this.op = op; 
        topT = super.valueType();
        top = ExpressionTypes.newType(topT, DEFAULT_PRECISION, DEFAULT_SCALE);
    }

    /**
     * This ctor is strictly to be used by DateTimeArithExpression where the
     * top type is set by the sub class because it doesn't follow *regular* rules
     * 
     * @param lhs
     * @param op
     * @param rhs
     * @param top 
     */
    protected ArithExpression (Expression lhs, ArithOp op, Expression rhs, AkType top)
    {
        super(top, lhs, rhs);
        this.op = op;
        if (lhs.valueType() != rhs.valueType()) // mis-matched arguments result in NULL
            topT = AkType.NULL;
        else
            topT = top;
        this.top = ExpressionTypes.newType(topT, DEFAULT_PRECISION, DEFAULT_SCALE);
    }
    
    @Override
    public void describe(StringBuilder sb)
    {
        sb.append(op);
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(op, this,childrenEvaluations(), top);
    }

    /**
     * Date/time types:
     *      DATE - DATE, TIME - TIME, YEAR - YEAR , DATETIME - DATETIME, => result is an INTERVAL_MILLIS
     *      DATE + INTERVAL_MILLIS => DATE, TIME + INTERVAL_MILLIS => TIME, ....etc
     *      INTERVAL_MILLIS + DATE => DATE, INTERVAL_MILLIS + TIME => TIME, ....etc
     *      DATE - INTERVAL_MILLIS => DATE, TIME - INTERVAL_MILLIS => TIME, ....etc
     *      INTERVAL_MILLIS + INTERVAL_MILLIS => INTERVAL_MILLIS
     *      INTERVAL_MILLIS - INTERVAL_MILLIS => INTERVAL_MILLIS
     *      INTERVAL_MILLIS * n (of anytypes :double, long ,...etc) => INTERVAL_MILLIS
     *      INTERVAL_MILLIS / n = INTERVAL_MILLIS
     *
     *  Regular types:
     *      Anything [+/*-]  DECIMAL => DECIMAL
     *      Anything (except DECIMAL) [+/*-] DOUBLE = > DOUBLE
     *      Anything (except DECIMAL and DOUBLE) [+/*-] U_BIGINT => U_BIGINT
     *      LONG [+/*-] LONG => LONG
     *
     * Anything else is unsupported
     *
     *
     * @param leftT
     * @param rightT
     * @param op
     * @return topType
     * @author VyNguyen
     *
     * * Why/how this works:
     *      1) find leftT and rightT's key
     *      2) find product of the two keys
     *      3) find sum of the two keys
     *
     *      if sum is neg then both are not supported since .get() only returns -1 if the types aren't in the map
     *      else
     *          if product is zero then at least one of the two is an INTERVAL_MILLIS (key of INTERVAL_MILLIS is zero)
     *              check sum :
     *              if sum is even (0, 2, 4...etc..) then the other operand is either an interval or date/time
     *                  check op : if it's anything other than + or - => throw exception
     *              if sum is odd (1, 3, 5...etc..) then the other operand is a regular numeric type
     *                  check of : if it's anything other than * or / => throw exception
     *          if product is positive, then both are supported types
     *              check product:
     *              if it's odd, then the two operands are both regular numeric (since the product of two numbers can only be odd if both the numbers are odd)
     *              if it's even, at least one of the two is date/time
     *                  the only legal case is when both operands are date, or time, and the operator is minus
     *                  else => throw exception
     *          if product is negative, then one is supported and one is NOT supported
     *              check sum:
     *              if sum is odd => one of the two operand is a date/time and the other is unsupported (since unsupported type get a key of -1, and date/time's key is an even. even -1 = odd)
     *                  in which case, throw an exception
     *              else if sum is even => unsupported and a numeric => return the numeric type
     */
    protected static AkType getTopType (AkType leftT, AkType rightT, ArithOp op)
    {
       if (leftT == AkType.NULL || rightT == AkType.NULL)  return AkType.NULL;
       String msg = leftT.name() + " " + op.opName() + " " + rightT.name();
       int l = SUPPORTED_TYPES.get(leftT), r = SUPPORTED_TYPES.get(rightT), sum = l + r;
       
       if (sum == HIGHEST_KEY && l * r == 0)  // two *kinds* of INTERVALs
           throw new InvalidArgumentTypeException(msg);  
       
       int l2 = l % HIGHEST_KEY, r2 = r % HIGHEST_KEY;
       int prod2 = l2*r2, sum2 = r2 + l2;
       
       if (sum2 <= -1 ) throw new InvalidArgumentTypeException(msg); // both are NOT supported || interval and a NOT supported
       if (prod2 == 0) // at least one is interval
       {
           if (sum2 %2 == 0) // datetime and interval alone
               switch (op.opName())
               {
                  case '-': if (r2 != 0) throw new InvalidArgumentTypeException(msg); // fall thru;  check if second operandis NOT interval E.g inteval - date? => nonsense
                  case '+': return SUPPORTED_TYPES.get(sum2 == 0 ? sum /2: sum2); // return date/time or interval
                  default: throw new InvalidArgumentTypeException(msg);
               }
            else // number and interval: an interval can be multiply with || divide by a number
            {
               AkType interval = l2 == 0 ? SUPPORTED_TYPES.get(l) : SUPPORTED_TYPES.get(r);
               char opName = op.opName();
               if ((opName == '/' || opName == '%' || opName == 'd')&& l2 == 0 || opName == '*') return interval;
               else throw new InvalidArgumentTypeException(msg);
            }
        }
        else if (prod2 > 0) // both are supported and none is an interval
        {
            if (prod2 % 2 == 1) // odd => numeric values only
            {
                int f = SUPPORTED_TYPES.get(AkType.FLOAT);
                return op.opName() == '/' && r > f && l > f // turn any exact type 
                        ? AkType.DOUBLE                     // into DOUBLE
                        : SUPPORTED_TYPES.get(l < r ? l : r);
            }
            else // even => at least one is datetime
            {
                if (l == r && op.opName() == '-') return AkType.INTERVAL_MILLIS;
                else throw new InvalidArgumentTypeException("");
            }
        }
        else // left || right is not supported
        {
            if( sum2 %2 == 1) throw new InvalidArgumentTypeException(msg); // date/times and unsupported
            else return SUPPORTED_TYPES.get(sum2+1);
        }   
    }

    protected static boolean isNumeric (AkType type)
    {
        int t = SUPPORTED_TYPES.get(type);
        if (t < 0) return false;
        else return t % 2 == 1;
    }
    
    protected static boolean isDateTime (AkType type)
    {
        int t = SUPPORTED_TYPES.get(type);
        if (t < 0) return false;
        else return t % 2 == 0;
    }
    
    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }
    
    @Override
    public String name()
    {
        return op.toString();
    }
    
    @Override
    public Explainer getExplainer(Map extraInfo)
    {
        ExpressionExplainer explainer = new ExpressionExplainer(Type.BINARY_OPERATOR, name(), extraInfo, children());
        if (op.isInfix())
            if (name().equals("d"))
            {
                explainer.addAttribute(Label.INFIX_REPRESENTATION, PrimitiveExplainer.getInstance("/"));
            }
            else
            {
                explainer.addAttribute(Label.INFIX_REPRESENTATION, PrimitiveExplainer.getInstance(name()));
            }
        if (op.isAssociative())
            explainer.addAttribute(Label.ASSOCIATIVE, PrimitiveExplainer.getInstance(true));
        return explainer;
    }
    
    /**
     * this is to be overridden by sub-classes to return a more appropriate ValueSource
     * @param op
     * @param topT
     * @return
     */
    protected InnerValueSource getValueSource (ArithOp op)
    {
        return new InnerValueSource(op, topT);
    }
  
    protected static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
        @Override
        public ValueSource eval() 
        {  
            if (valueSource.getConversionType() == AkType.NULL)
                return NullValueSource.only();
            valueSource.setOperands(left(), right(), top);
            return valueSource;
        }
        
        protected InnerEvaluation (ArithOp op, ArithExpression ex,
                List<? extends ExpressionEvaluation> children, ExpressionType t)
        {
            super(children);
            valueSource = ex.getValueSource(op);
            top = t;
        }
        
        private final ExpressionType top;
        protected final InnerValueSource valueSource;
    }

   protected static class InnerValueSource extends AbstractArithValueSource
   {
       private final ArithOp op;
       protected ValueSource left;
       protected ValueSource right;
       private AkType topT;
       private ExpressionType top;
       public InnerValueSource (ArithOp op,  AkType topT )
       {
           this.op = op;
           this.topT = topT;
       }
       
       public void setOperands (ValueSource left, ValueSource right, ExpressionType t)
       {
           ArgumentValidation.notNull("Left", left);
           ArgumentValidation.notNull("Right", right);
           this.left = left;
           this.right = right;
           top = t;
       }
       
       @Override
       public AkType getConversionType () 
       {
          return topT; 
       }

        @Override
        protected long rawLong() 
        {
            return op.evaluate(
                    Extractors.getLongExtractor(
                        (left.getConversionType() == AkType.VARCHAR ? topT : left.getConversionType())).getLong(left),
                    Extractors.getLongExtractor(
                        (right.getConversionType() == AkType.VARCHAR ? topT : right.getConversionType())).getLong(right),
                    top);
        }

        @Override
        protected double rawDouble()
        {
            return op.evaluate(Extractors.getDoubleExtractor().getDouble(left),
                    Extractors.getDoubleExtractor().getDouble(right), top);
        }  

        @Override
        protected BigInteger rawBigInteger() 
        {                   
           return op.evaluate(Extractors.getUBigIntExtractor().getObject(left),
                   Extractors.getUBigIntExtractor().getObject(right), top);
        }

        @Override
        protected BigDecimal rawDecimal() 
        {
            return op.evaluate(Extractors.getDecimalExtractor().getObject(left),
                    Extractors.getDecimalExtractor().getObject(right), top);
        }

        @Override
        protected long rawInterval ()
        {
            /*
             * The rawInterval() is called only if the top's type is INTERVAL_MILLIS, which  only happens when
             *      one of the two operands is an interval and the other is a regular numeric value
             *      or both operands are of the same date/time type, that is, both are date, or both are time, etc
             *
             * DECIMAL's key in SUPPORTED is 1, DOUBLE's key is 3, [...etc...] and INTERVAL_MILLIS'S is 0.
             * So if the difference is -1 or 1 => rawDecimal
             *    if  //               -3 or 3 => rawDouble
             *      [...etc...]
             *    else  (when the difference is either 0 or something different than the above)
                    : must be date/times => rawLong
             *
             */
            int l = SUPPORTED_TYPES.get(left.getConversionType()), r = SUPPORTED_TYPES.get(right.getConversionType());
            int pos = l% HIGHEST_KEY - r % HIGHEST_KEY;
            switch (pos = pos > 0 ? pos : - pos)
            {
                case 1:     return rawDecimal().longValue();
                case 3:     
                case 5:     return (long)rawDouble();
                case 7:     return rawBigInteger().longValue();
                case 9:
                case 11:    return rawLong();
                case 0:     if (l == 0 || l == HIGHEST_KEY) return rawLong();// left and right are intervals
                            else return doArithMillis(pos); // left and right are date/times
                default:    if (l == 0 || r == 0) return doArithMillis(pos); // left is date/time and right is interval or vice versa
                            else return doArithMonth(); 
            }
        }
        
        protected long doArithMillis (int pos)
        {
            LongExtractor lEx = Extractors.getLongExtractor(left.getConversionType());
            LongExtractor rEx = Extractors.getLongExtractor(right.getConversionType());
            long leftUnix = lEx.stdLongToUnix(lEx.getLong(left));
            long rightUnix = rEx.stdLongToUnix(rEx.getLong(right));               
            return Extractors.getLongExtractor(SUPPORTED_TYPES.get(pos)). 
                    unixToStdLong(op.evaluate(leftUnix, rightUnix, top));
        }
        
     
        protected long doArithMonth ()
        {
            long ymd_hms[];
            long interval;
            LongExtractor extract = Extractors.getLongExtractor(topT);
   
            if (left.getConversionType() == AkType.INTERVAL_MONTH)
            {
                interval = left.getInterval_Month();
                ymd_hms = extract.getYearMonthDayHourMinuteSecond(extract.getLong(right));
            }
            else
            {
                interval = right.getInterval_Month();
                ymd_hms = extract.getYearMonthDayHourMinuteSecond(extract.getLong(left));
            }

            if (!vallidDayMonth(ymd_hms[0], ymd_hms[1], ymd_hms[2]))
            {
                topT = AkType.NULL;
                throw new ValueSourceIsNullException();
            }

            if (op.opName() == '+' && interval >= 0)
            {
                ymd_hms[1] += interval;
                if (ymd_hms[1] > 12)
                {
                    long tempMonth = ymd_hms[1] % 12;
                    tempMonth = tempMonth == 0 ? 12 : tempMonth;
                    ymd_hms[0] += (ymd_hms[1] - tempMonth) /12;
                    ymd_hms[1] = tempMonth;
                }
            }
            else
            {
                ymd_hms[1] -= (interval *= op.opName() == '+' ? -1 : 1) % 12;
                ymd_hms[0] -= interval / 12;
                if (ymd_hms[1] <= 0)
                {
                    ymd_hms[1] += 12;
                    --ymd_hms[0];
                }
            }
            
            // adjust day value
            switch ((int)ymd_hms[1])
            {
                case 2: ymd_hms[2] = Math.min(ymd_hms[0] % 400 == 0 || ymd_hms[0] % 4 == 0  && ymd_hms[0] % 100 != 0
                                                                    ? 29 : 28,
                                                                    ymd_hms[2]);
                        break;
                case 4:
                case 6:
                case 9:
                case 11: ymd_hms[2] = Math.min(30, ymd_hms[2]);
                         break;
                case 3:
                case 1:
                case 5:
                case 7:
                case 8:
                case 10:
                case 12: ymd_hms[2] = Math.min(31, ymd_hms[2]);
                         break;
                default: throw new InvalidParameterValueException();
            }
            return extract.getEncoded(ymd_hms);
        }

        protected static boolean vallidDayMonth (long y, long m, long d)
        {
            switch ((int)m)
            {
                case 2:     return d <= (y % 400 == 0 || y % 4 == 0 && y % 100 != 0 ? 29L : 28L);
                case 4:
                case 6:
                case 9:
                case 11:    return d <= 30;
                case 3:
                case 1:
                case 5:
                case 7:
                case 8:
                case 10:
                case 12:    return d <= 31;
                default:    return false;
            }
        }

        @Override
        public boolean isNull() 
        {
            return left.isNull() || right.isNull();
        }       
   }
}
