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

package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.common.BigDecimalWrapper;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimal;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimal.Attrs;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimalWrapper;
import com.akiban.server.types3.pvalue.PValueTarget;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CastUtils
{
    public static long round (long max, long min, double val, TExecutionContext context)
    {
        long rounded = Math.round(val);
        
        if (Double.compare(rounded, val) != 0)
            context.reportTruncate(Double.toString(val), Long.toString(rounded));
        return getInRange(max, min, rounded, context);
    }

    public static long getInRange (long max, long min, long val, TExecutionContext context)
    {
        if (val > max)
        {
            context.reportTruncate(Long.toString(val), Long.toString(max));
            return max;
        }
        else if (val < min)
        {
            context.reportTruncate(Long.toString(val), Long.toString(min));
            return min;
        }
        else
            return val;
    }

    public static long getInRange (long max, long min, long val, long defaultVal, TExecutionContext context)
    {
        if (val > max)
        {
            context.reportTruncate(Long.toString(val), Long.toString(defaultVal));
            return defaultVal;
        }
        else if (val < min)
        {
            context.reportTruncate(Long.toString(val), Long.toString(defaultVal));
            return defaultVal;
        }
        else
            return val;
    }
     
     public static String getNum(int scale, int precision)
    {
        assert precision >= scale : "precision has to be >= scale";
        
        char val[] = new char[precision + 1];
        Arrays.fill(val, '9');
        val[precision - scale] = '.';
        
        return new String(val);
    }
  
    public static void doCastDecimal(TExecutionContext context,
                            BigDecimalWrapper num,
                            PValueTarget out)
    {
        int pre = num.getPrecision();
        int scale = num.getScale();

        int expectedPre = context.outputTInstance().attribute(Attrs.PRECISION);
        int expectedScale = context.outputTInstance().attribute(Attrs.SCALE);

        BigDecimalWrapper meta[] = (BigDecimalWrapper[]) context.outputTInstance().getMetaData();

        if (meta == null)
        {
            // compute the max value:
            meta = new BigDecimalWrapper[2];
            meta[MBigDecimal.MAX_INDEX] = new MBigDecimalWrapper(getNum(expectedScale, expectedPre));
            meta[MBigDecimal.MIN_INDEX] = meta[MBigDecimal.MAX_INDEX].negate();

            context.outputTInstance().setMetaData(meta);
        }

        if (num.compareTo(meta[MBigDecimal.MAX_INDEX]) >= 0)
            out.putObject(meta[MBigDecimal.MAX_INDEX]);
        else if (num.compareTo(meta[MBigDecimal.MIN_INDEX]) <= 0)
            out.putObject(meta[MBigDecimal.MIN_INDEX]);
        else if (scale > expectedScale) // check the sacle
            out.putObject(num.round(expectedPre, expectedScale));
        else // else put the original value
            out.putObject(num);
    }

    public static MBigDecimalWrapper parseDecimalString (String st, TExecutionContext context)
    {
        Matcher m = DOUBLE_PATTERN.matcher(st);
        
        m.lookingAt();
        String truncated = st.substring(0, m.end());
        
        if (!truncated.equals(st))
            context.reportTruncate(st, truncated);
        
        MBigDecimalWrapper ret = MBigDecimalWrapper.ZERO;
        try
        {
            ret = new MBigDecimalWrapper(truncated);
        }
        catch (NumberFormatException e)
        {
            context.reportBadValue(e.getMessage());
        }
        return ret;
    }
    /**
     * Parse the st for a double value
     * MySQL compat in that illegal digits will be truncated and won't cause
     * NumberFormatException
     * 
     * @param st
     * @param context
     * @return 
     */
    public static double parseDoubleString(String st, TExecutionContext context)
    {      
        Matcher m = DOUBLE_PATTERN.matcher(st);

        m.lookingAt();
        String truncated = st.substring(0, m.end());

        if (!truncated.equals(st))
        {
            context.reportTruncate(st, truncated);
        }

        double ret = 0;
        try
        {
            ret = Double.parseDouble(truncated);
        }
        catch (NumberFormatException e)
        {
            context.reportBadValue(e.getMessage());
        }

       return ret;
    }
    
    public static long parseInRange(String st, long max, long min, TExecutionContext context)
    {
        String truncated;

        // first attempt
        try
        {
            return CastUtils.getInRange(max, min, Long.parseLong(st), context);
        }
        catch (NumberFormatException e)
        {
            truncated = CastUtils.truncateNonDigits(st, context);
        }

        // second attempt
        return CastUtils.getInRange(max, min, Long.parseLong(truncated), context);
    }
    
    /**
     * Truncate non-digits part of a string that represents an integer. This also rounds the string. The rounding
     * is here (as opposed to at the call site) for ease of use, especially when parsing this as an unsigned long.
     * If the input string doesn't contain a valid integer, returns "0".
     * @param st the string to parse
     * @return a non-empty, non-null string which contains all digits, with a possible leading '-'
     */
    public static String truncateNonDigits(String st, TExecutionContext context)
    {
        final int firstChar = (st.charAt(0) == '-') ? 1 : 0;
        int truncatedLength = firstChar;
        boolean needsRoundingUp = false;
        for(int max=st.length(); truncatedLength < max; ++truncatedLength) {
            char c = st.charAt(truncatedLength);
            if (!Character.isDigit(c)) {
                needsRoundingUp = (c == '.') && isFiveOrHigher(st, truncatedLength + 1);
                break;
            }
        }
        String ret;
        if (needsRoundingUp) {
            StringBuilder sb = new StringBuilder(truncatedLength+2); // 1 for '-', 1 for the carry digit
            // Go right to left on the string, not counting the leading '-'. Assume every char is a digit. If you see
            // a '9', set it to 0 and continue the loop. Otherwise, set needsRoundingUp to false and break.
            // Once the loop is done, if needsRoundingUp is still true, append a 1. Finally, append the '-' if we need
            // it, reverse the string, and return it.
            for (int i = truncatedLength-1; i >= firstChar; --i) {
                char c = st.charAt(i);
                assert (c >= '0') && (c <= '9') : c + " at index " + i + " of: " + st;
                if (c == '9') {
                    sb.append('0');
                }
                else {
                    ++c;
                    sb.append(c);
                    needsRoundingUp = false;
                    break;
                }
            }
            if (needsRoundingUp)
                sb.append('1');
            if (firstChar == 1)
                sb.append('-');
            sb.reverse();
            ret = sb.toString();
        }
        else {
            ret = st.substring(0, truncatedLength);
        }
        return ret;
    }

    private static boolean isFiveOrHigher(String string, int index) {
        if (index >= string.length())
            return false;
        char c = string.charAt(index);
        return (c >= '5') && (c <= '9');
    }

    private static final Pattern DOUBLE_PATTERN = Pattern.compile("([-+]?\\d*)(\\.?\\d+)?([eE][-+]?\\d+)?");
}
