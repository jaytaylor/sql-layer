/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.types.mcompat.mcasts;

import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.BigDecimalWrapperImpl;
import com.foundationdb.server.types.common.types.DecimalAttribute;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.value.ValueTarget;
import com.google.common.primitives.UnsignedLongs;
import java.math.BigDecimal;
import java.math.RoundingMode;
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
                            ValueTarget out)
    {
        int pre = num.getPrecision();
        int scale = num.getScale();

        int expectedPre = context.outputType().attribute(DecimalAttribute.PRECISION);
        int expectedScale = context.outputType().attribute(DecimalAttribute.SCALE);

        BigDecimalWrapper meta[] = (BigDecimalWrapper[]) context.outputType().getMetaData();

        if (meta == null)
        {
            // compute the max value:
            meta = new BigDecimalWrapper[2];
            meta[TBigDecimal.MAX_INDEX] = new BigDecimalWrapperImpl(getNum(expectedScale, expectedPre));
            meta[TBigDecimal.MIN_INDEX] = new BigDecimalWrapperImpl(meta[TBigDecimal.MAX_INDEX].asBigDecimal().negate());

            context.outputType().setMetaData(meta);
        }

        if (num.compareTo(meta[TBigDecimal.MAX_INDEX]) >= 0)
            out.putObject(meta[TBigDecimal.MAX_INDEX]);
        else if (num.compareTo(meta[TBigDecimal.MIN_INDEX]) <= 0)
            out.putObject(meta[TBigDecimal.MIN_INDEX]);
        else if (scale != expectedScale) // check the sacle
            out.putObject(num.round(expectedScale));
        else // else put the original value
            out.putObject(num);
    }

    public static BigDecimalWrapperImpl parseDecimalString (String st, TExecutionContext context)
    {
        Matcher m = DOUBLE_PATTERN.matcher(st);
        
        m.lookingAt();
        String truncated = st.substring(0, m.end());
        
        if (!truncated.equals(st))
            context.reportTruncate(st, truncated);
        
        BigDecimalWrapperImpl ret = BigDecimalWrapperImpl.ZERO;
        try
        {
            ret = new BigDecimalWrapperImpl(truncated);
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
        double ret = 0;
        Matcher m = DOUBLE_PATTERN.matcher(st);

        if (m.lookingAt())
        {
            String truncated = st.substring(0, m.end());

            if (!truncated.equals(st))
            {
                context.reportTruncate(st, truncated);
            }

            try
            {
                ret = Double.parseDouble(truncated);
            }
            catch (NumberFormatException e)
            {
                context.reportBadValue(e.getMessage());
            }
        }
        else
            context.reportBadValue(st);

        return ret;
    }

    public static long parseInRange(String st, long max, long min, TExecutionContext context)
    {
        Object truncated;

        // first attempt
        try
        {
            return CastUtils.getInRange(max, min, Long.parseLong(st), context);
        }
        catch (NumberFormatException e) // This could be an overflow, but there is no way to know
        {
            truncated = CastUtils.truncateNonDigits(st, context);
        }

        // second attemp
        if (truncated instanceof String)
        {
            String truncatedStr = (String)truncated;
            try
            {
                return CastUtils.getInRange(max, min, Long.parseLong(truncatedStr), context);
            }
            catch (NumberFormatException e) // overflow
            {
                context.reportOverflow(e.getMessage());
                
                // check wether the number is too big or too small
                char first = truncatedStr.charAt(0);
                if (first == '-')
                    return getInRange(max, min, Long.MIN_VALUE, context);
                else
                    return getInRange(max, min, Long.MAX_VALUE, context);
            }
        }
        else // must be a BigDecimal object
        {
            BigDecimal num = (BigDecimal)truncated;
            
            // check overflow
            if (num.compareTo(MAX_LONG) > 0)
            {
                context.reportTruncate(st, Long.toString(max));
                return max;
            }
            else if (num.compareTo(MIN_LONG) < 0)
            {
                context.reportTruncate(st, Long.toString(min));
                return min;
            }

            try
            {
                return getInRange(max, min, num.longValueExact(), context);
            }
            catch (ArithmeticException e) // has non-zero fractional parts
            {
                long ret = num.setScale(0, RoundingMode.HALF_UP).longValue();
                context.reportTruncate(st, Long.toString(ret));
                return getInRange(max, min, ret, context);
            }
        }
    }
    
    public static long parseUnsignedLong(String st, TExecutionContext context)
    {
        Object truncated = CastUtils.truncateNonDigits(st, context);

        if (truncated instanceof String)
            st = (String)truncated;
        else
            st = CastUtils.truncateNonDigitPlainString(((BigDecimal)truncated).toPlainString(),
                                                       context);

        long value;
        try 
        {
            value = UnsignedLongs.parseUnsignedLong(st);
        } catch (NumberFormatException e) { // overflow error
            context.reportOverflow(e.getMessage());

            // check wether the value is too big or too small
            if (st.charAt(0) == '-')
                value = 0;
            else
                value = UnsignedLongs.MAX_VALUE;
        }
        return value;
    }
    
    /**
     * Truncate non-digits part of a string that represents an integer. This also rounds the string. The rounding
     * is here (as opposed to at the call site) for ease of use, especially when parsing this as an unsigned long.
     * If the input string doesn't contain a valid integer, returns "0".
     * @param st the string to parse
     * @return a non-empty, non-null string which contains all digits, with a possible leading '-'
     */
    public static Object truncateNonDigits(String st, TExecutionContext context)
    {
        if (st.isEmpty())
            return "0";

        Matcher m = DOUBLE_PATTERN.matcher(st);
        String truncated;
        int last;
        if (m.lookingAt() && !(truncated = st.substring(0, last = m.end())).isEmpty())
        {
            --last; // m.end() returns an offset from the beginning, not an index
            if (truncated.charAt(last) != st.charAt(last))
                context.reportTruncate(st, truncated);
            
            // If the exponent exists, use BigDecimal
            if ( m.group(EXP_PART) != null)
                return new BigDecimal(st);

            // otherwise, truncate  non-digit chars
            return truncateNonDigitPlainString(truncated, context);
        }
        else // not a valid numeric string
        {
            context.reportBadValue(st);
            return "0";
        }
        
    }

    public static String truncateNonDigitPlainString(String st, TExecutionContext context)
    {
        final int max = st.length();
        if (max == 0)
            return "0";
        final boolean neg;
        final int firstIndex = ((neg = st.charAt(0) == '-') || st.charAt(0) == '+') ? 1 : 0;
        boolean needsRoundingUp = false; // whether the number ends in "\.[5-9]"
        int truncatedLength;
        for (truncatedLength = firstIndex; truncatedLength < max; ++truncatedLength)
        {
            char c = st.charAt(truncatedLength);
            if (!Character.isDigit(c))
            {
                needsRoundingUp = (c == '.') && isFiveOrHigher(st, truncatedLength + 1);
                break;
            }
        }

        if (truncatedLength == firstIndex && !needsRoundingUp)
            return "0"; // no digits

        String ret;
        if (needsRoundingUp)
        {
            StringBuilder sb = new StringBuilder(truncatedLength + 2); // 1 for '-', 1 for the carry digit
            // Go right to left on the string, not counting the leading '-'. Assume every char is a digit. If you see
            // a '9', set it to 0 and continue the loop. Otherwise, set needsRoundingUp to false and break.
            // Once the loop is done, if needsRoundingUp is still true, append a 1. Finally, append the '-' if we need
            // it, reverse the string, and return it.
            for (int i = truncatedLength - 1; i >= firstIndex; --i)
            {
                char c = st.charAt(i);
                assert (c >= '0') && (c <= '9') : c + " at index " + i + " of: " + st;
                if (needsRoundingUp && c == '9')
                {
                    sb.append('0');
                }
                else
                {
                    if (needsRoundingUp)
                        ++c;
                    sb.append(c);
                    needsRoundingUp = false;
                }
            }
            if (needsRoundingUp)
                sb.append('1');
            if (neg)
                sb.append('-');
            sb.reverse();
            ret = sb.toString();
        }
        else
        {
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

    /** Clamp {@code raw} to the range of [0,255] representing 0000 or an offset from 1900. */
    public static short adjustYear(long raw, TExecutionContext context) {
        // Too small, too big or invalid 2 digit/out of 4 digit range
        if(raw < 0 || raw > 2155 || (raw >= 100 && raw <= 1900)) {
            context.reportTruncate(String.valueOf(raw), "0000");
            return 0;
        }
        if(raw == 0) {
            return 0;
        }
        // 2000 + raw
        if(raw <= 69) {
            return (short)(100 + raw);
        }
        // 1900 + raw
        if(raw <= 99) {
            return (short)raw;
        }
        // 1901-2155
        return (short)(raw - 1900);
    }
    private static final Pattern DOUBLE_PATTERN = Pattern.compile("([-+]?\\d*)(\\.?\\d+)?([eE][-+]?\\d+)?");

    private static int WHOLE_PART = 1;
    private static int FLOAT_PART = 2;
    private static int EXP_PART = 3;
   
    public static final long MAX_TINYINT = 127;
    public static final long MAX_UNSIGNED_TINYINT = 255;
    public static final long MIN_TINYINT = -128;
    
    public static final long MAX_SMALLINT = 32767;
    public static final long MAX_UNSIGNED_SMALLINT = 65535;
    public static final long MIN_SMALLINT = -32768;
    
    public static final long MAX_MEDINT = 8388607;
    public static final long MAX_UNSIGNED_MEDINT = 16777215;
    public static final long MIN_MEDINT = -8388608;
    
    public static final long MAX_INT = 2147483647;
    public static final long MAX_UNSIGNED_INT = 4294967295L;
    public static final long MIN_INT = -2147483648;
    
    public static final long MAX_BIGINT = 9223372036854775807L;
    public static final long MIN_BIGINT = -9223372036854775808L;

    private static final BigDecimal MAX_LONG = BigDecimal.valueOf(Long.MAX_VALUE);
    private static final BigDecimal MIN_LONG = BigDecimal.valueOf(Long.MIN_VALUE);
}
