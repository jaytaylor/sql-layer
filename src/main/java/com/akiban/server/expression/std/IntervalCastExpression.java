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

import com.akiban.qp.exec.Plannable;
import com.akiban.server.error.InvalidCharToNumException;
import com.akiban.server.error.InvalidArgumentTypeException;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.types.extract.Extractors;
import java.util.HashMap;
import com.akiban.sql.types.TypeId;
import com.akiban.server.error.InvalidIntervalFormatException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.NullValueSource;

import static com.akiban.server.types.AkType.*;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.Label;
import com.akiban.sql.optimizer.explain.PrimitiveExplainer;
import java.util.Map;

public class IntervalCastExpression extends AbstractUnaryExpression
{
    protected static enum EndPoint
    {
        YEAR(INTERVAL_MONTH), 
        MONTH(INTERVAL_MONTH), 
        YEAR_MONTH(INTERVAL_MONTH), 
        DAY(INTERVAL_MILLIS), 
        HOUR(INTERVAL_MILLIS), 
        MINUTE(INTERVAL_MILLIS), 
        SECOND(INTERVAL_MILLIS), 
        DAY_HOUR(INTERVAL_MILLIS), 
        DAY_MINUTE(INTERVAL_MILLIS), 
        DAY_SECOND(INTERVAL_MILLIS),
        HOUR_MINUTE(INTERVAL_MILLIS), 
        HOUR_SECOND(INTERVAL_MILLIS),
        MINUTE_SECOND(INTERVAL_MILLIS);
        
        private EndPoint (AkType type)
        {
            this.type = type;
        }
        
        final AkType type;
    }
    
    private static final HashMap<TypeId,EndPoint> ID_MAP = new HashMap<TypeId,EndPoint>();
    static
    {
        ID_MAP.put(TypeId.INTERVAL_YEAR_ID, EndPoint.YEAR);
        ID_MAP.put(TypeId.INTERVAL_YEAR_MONTH_ID, EndPoint.YEAR_MONTH);
        ID_MAP.put(TypeId.INTERVAL_MONTH_ID, EndPoint.MONTH);
        ID_MAP.put(TypeId.INTERVAL_DAY_ID, EndPoint.DAY);
        ID_MAP.put(TypeId.INTERVAL_HOUR_ID, EndPoint.HOUR);
        ID_MAP.put(TypeId.INTERVAL_MINUTE_ID, EndPoint.MINUTE);
        ID_MAP.put(TypeId.INTERVAL_SECOND_ID, EndPoint.SECOND);
        ID_MAP.put(TypeId.INTERVAL_DAY_SECOND_ID, EndPoint.DAY_SECOND);
        ID_MAP.put(TypeId.INTERVAL_DAY_MINUTE_ID, EndPoint.DAY_MINUTE);
        ID_MAP.put(TypeId.INTERVAL_DAY_HOUR_ID, EndPoint.DAY_HOUR);
        ID_MAP.put(TypeId.INTERVAL_HOUR_MINUTE_ID, EndPoint.HOUR_MINUTE);
        ID_MAP.put(TypeId.INTERVAL_HOUR_SECOND_ID, EndPoint.HOUR_SECOND);
        ID_MAP.put(TypeId.INTERVAL_MINUTE_SECOND_ID, EndPoint.MINUTE_SECOND);
    }

    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private final EndPoint endPoint;
        private static final long MULS[] = { 86400000L, 3600000L, 60000L, 1000L};
        
        public InnerEvaluation (ExpressionEvaluation ev, EndPoint endPoint)
        {
            super(ev);
            this.endPoint = endPoint;
        }

        @Override
        public ValueSource eval() 
        {
            ValueSource source = operand();
            if (source.isNull()) return NullValueSource.only();

            String interval = null;
            Double result = null;
            AkType sourceType = source.getConversionType();
            double sign = 1;

            try
            {
                if (sourceType == AkType.VARCHAR)
                {
                    interval = source.getString().trim();
                    char ch = '\0';
                    if (!interval.isEmpty() &&
                            (ch = interval.charAt(0)) == '-' || ch == '+')
                    {
                        sign = ch == '-' ? -1 : 1;
                        interval = interval.substring(1, interval.length()); 
                    }
                }
                else if (!Converters.isConversionAllowed(sourceType, AkType.LONG))
                    throw new InconvertibleTypesException(sourceType, endPoint.type);
                else
                    result = Extractors.getDoubleExtractor().getDouble(source);

                switch(endPoint)
                {
                    case YEAR:
                        if (result == null)
                            result = Double.parseDouble(interval);
                        result *= 12L * sign;
                        break;
                    case MONTH: 
                        if (result == null)
                            result = sign * Double.parseDouble(interval);
                        break;
                    case YEAR_MONTH: 
                        String yr_mth[] = interval.split("-");
                        if (yr_mth.length != 2) 
                            throw new InvalidIntervalFormatException (endPoint.name(), interval);
                        result =  sign * (Long.parseLong(yr_mth[0]) * 12 + Long.parseLong(yr_mth[1]));
                        break;
                    case DAY:
                        if (result == null)
                            result = Double.parseDouble(interval);
                        result *= MULS[0] * sign;
                        break;
                    case HOUR:
                        if (result == null)
                            result = Double.parseDouble(interval); 
                        result *= MULS[1]* sign;
                        break;
                    case MINUTE:
                        if (result == null)
                            result = Double.parseDouble(interval);
                        result *=  MULS[2] * sign;
                        break;
                    case SECOND:
                        result = Extractors.getDoubleExtractor().getDouble(source) * 1000;
                        break;
                    case DAY_HOUR:
                        result = sign * getResult(0, interval, "\\s+", 2, false);
                        break;
                    case DAY_MINUTE:
                        result = sign * getResult(0, interval, "\\s+|:", 3, false);
                        break;
                    case DAY_SECOND:
                        result = sign * getResult(0, interval, "\\s+|:", 4, true);
                        break;
                    case HOUR_MINUTE:
                        result = sign * getResult(1,interval, ":", 2, false);
                        break;
                    case HOUR_SECOND:
                        result = sign * getResult(1, interval, ":", 3, true);
                        break;
                    case MINUTE_SECOND:
                        result = sign * getResult(2, interval, ":", 2, true);
                        break;
                    default:
                        throw new InvalidArgumentTypeException ("INTERVAL _" + endPoint.name() + " is not supported");
                }
                
                valueHolder().putRaw(endPoint.type, result.longValue());
                return valueHolder();
            } 
            catch (NumberFormatException ex)
            {
                throw new InvalidIntervalFormatException (endPoint.name(), interval);
            }
            catch (NullPointerException ex) 
            {
                throw new InvalidIntervalFormatException (endPoint.name(), "");
            }
            catch (InvalidCharToNumException ex)
            {
                throw new InvalidIntervalFormatException (endPoint.name(), interval);
            }
        }
        
        private long getResult (int start, String interval, String del, int expSize, boolean lastIsSec )
        {
            String strings[] = interval.split(del);
            if (strings.length != expSize) 
                throw new InvalidIntervalFormatException (endPoint.name(), interval);
            long res = 0;
            for (int n = 0, m = start; n < expSize - (lastIsSec ? 1 : 0); ++n, ++m)
                res += Long.parseLong(strings[n]) * MULS[m];
            if (lastIsSec)
                res += Math.round(Double.parseDouble(strings[expSize-1]) * 1000L);
            
            return res;
        }
        
    }
    
    private final EndPoint endPoint;
    protected IntervalCastExpression (Expression str, EndPoint endPoint)
    {
        super(endPoint.type, str);
        this.endPoint = endPoint;
    }

    public IntervalCastExpression (Expression str, TypeId id)
    {
        this (str, getEndPoint(id));
    }

    static private EndPoint getEndPoint (TypeId id)
    {
        EndPoint ep = ID_MAP.get(id);
        if (ep == null)
            throw new InvalidArgumentTypeException ("Unsupported INTERVAL format - " + id.getSQLTypeName());
        else
            return ep;
    }
    @Override
    public String name() 
    {
        return "CAST_INTERVAL_" + endPoint;
    }
    
    @Override
    public Explainer getExplainer(Map<Object, Explainer> extraInfo)
    {
        Explainer ex = super.getExplainer(extraInfo);
        ex.addAttribute(Label.OUTPUT_TYPE, PrimitiveExplainer.getInstance(endPoint.type.name()));
        return ex;
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(operandEvaluation(), endPoint);
    } 
}
