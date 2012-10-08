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

package com.akiban.server.types3.aksql.aktypes;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.IllegalNameException;
import com.akiban.server.types3.TBundleID;
import com.akiban.server.types3.TClassBase;
import com.akiban.server.types3.TClassFormatter;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TFactory;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TParser;
import com.akiban.server.types3.aksql.AkBundle;
import com.akiban.server.types3.aksql.AkCategory;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;
import com.akiban.util.AkibanAppender;
import com.google.common.math.LongMath;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AkInterval extends TClassBase {

    private static TClassFormatter monthsFormatter = new TClassFormatter() {
        @Override
        public void format(TInstance instance, PValueSource source, AkibanAppender out) {
            long months = source.getInt64();

            long years = months / 12;
            months -= (years * 12);

            Formatter formatter = new Formatter(out.getAppendable());
            formatter.format("INTERVAL '%d-%d", years, months);
        }

        @Override
        public void formatAsLiteral(TInstance instance, PValueSource source, AkibanAppender out) {
            long value = source.getInt64();
            Formatter formatter = new Formatter(out.getAppendable());
            out.append("INTERVAL '");
            long years, months;
            if (value < 0) {
                out.append('-');
                months = -value;
            }
            else {
                months = value;
            }
            years = months / 12;
            months -= years * 12;
            String hi = null, lo = null;
            if (years > 0) {
                formatter.format("%d", years);
                hi = lo = "YEAR";
            }
            if ((months > 0) || (hi == null)) {
                if (hi != null) {
                    formatter.format("-%02d", months);
                }
                else {
                    formatter.format("%d", months);
                }
                lo = "MONTH";
                if (hi == null) hi = lo;
            }
            out.append("' ");
            out.append(hi);
            if (hi != lo) {
                out.append(" TO ");
                out.append(lo);
            }
        }

        @Override
        public void formatAsJson(TInstance instance, PValueSource source, AkibanAppender out) {
            long months = source.getInt64();
            out.append(Long.toString(months));
        }
    };

    private static TClassFormatter secondsFormatter = new TClassFormatter() {
        @Override
        public void format(TInstance instance, PValueSource source, AkibanAppender out) {
            long micros = secondsIntervalAs(source, TimeUnit.MICROSECONDS);

            long days = secondsIntervalAs(micros, TimeUnit.DAYS);
            micros -= TimeUnit.DAYS.toMicros(days);

            long hours = secondsIntervalAs(micros, TimeUnit.HOURS);
            micros -= TimeUnit.HOURS.toMicros(hours);

            long minutes = secondsIntervalAs(micros, TimeUnit.MINUTES);
            micros -= TimeUnit.MINUTES.toMicros(minutes);

            long seconds = secondsIntervalAs(micros, TimeUnit.SECONDS);
            micros -= TimeUnit.SECONDS.toMicros(seconds);

            Formatter formatter = new Formatter(out.getAppendable());
            formatter.format("INTERVAL '%d %d:%d:%d.%05d", days, hours, minutes, seconds, micros);
        }

        @Override
        public void formatAsLiteral(TInstance instance, PValueSource source, AkibanAppender out) {
            long value = secondsIntervalAs(source, TimeUnit.MICROSECONDS);
            Formatter formatter = new Formatter(out.getAppendable());
            out.append("INTERVAL '");
            long days, hours, mins, secs, micros;
            if (value < 0) {
                out.append('-');
                micros = -value;
            }
            else {
                micros = value;
            }
            // Could be data-driven, but just enough special cases that
            // that would be pretty complicated.
            secs = micros / 1000000;
            micros -= secs * 1000000;
            mins = secs / 60;
            secs -= mins * 60;
            hours = mins / 60;
            mins -= hours * 60;
            days = hours / 24;
            hours -= days * 24;
            String hi = null, lo = null;
            if (days > 0) {
                formatter.format("%d", days);
                hi = lo = "DAY";
            }
            if ((hours > 0) ||
                ((hi != null) && ((mins > 0) || (secs > 0) || (micros > 0)))) {
                if (hi != null) {
                    formatter.format(":%02d", hours);
                }
                else {
                    formatter.format("%d", hours);
                }
                lo = "HOUR";
                if (hi == null) hi = lo;
            }
            if ((mins > 0) ||
                ((hi != null) && ((secs > 0) || (micros > 0)))) {
                if (hi != null) {
                    formatter.format(":%02d", mins);
                }
                else {
                    formatter.format("%d", mins);
                }
                lo = "MINUTE";
                if (hi == null) hi = lo;
            }
            if ((secs > 0) || (hi == null) || (micros > 0)) {
                if (hi != null) {
                    formatter.format(":%02d", secs);
                }
                else {
                    formatter.format("%d", secs);
                }
                lo = "SECOND";
                if (hi == null) hi = lo;
            }
            if (micros > 0) {
                if ((micros % 1000) == 0)
                    formatter.format(".%03d", micros / 1000);
                else
                    formatter.format(".%06d", micros);
            }
            out.append("' ");
            out.append(hi);
            if (hi != lo) {
                out.append(" TO ");
                out.append(lo);
            }
        }

        @Override
        public void formatAsJson(TInstance instance, PValueSource source, AkibanAppender out) {
            long value = secondsIntervalAs(source, TimeUnit.MICROSECONDS);
            long secs = value / 1000000;
            long micros = value % 1000000;
            Formatter formatter = new Formatter(out.getAppendable());
            formatter.format("%d.%06d", secs, micros);
        }
    };

    /**
     * A MONTHS interval, whose 64-bit value represents number of months.
     */
    public static final AkInterval MONTHS = new AkInterval(
            AkBundle.INSTANCE.id(),
            "interval months",
            AkCategory.DATE_TIME,
            MonthsAttrs.class,
            monthsFormatter,
            1,
            1,
            8,
            PUnderlying.INT_64,
            MonthsAttrs.FORMAT,
            AkIntervalMonthsFormat.values());

    /**
     * <p>A SECONDS interval, whose value 64-bit value does <em>not</em> necessarily represent number of seconds.
     * In fact, it almost definitely is not number of seconds; instead, the value is in some private unit. That said,
     * it will still be a unit of time, so you can work with it intuitively. For instance, adding two values of
     * the raw form will result in a number in the same unit that represents the sum of the two durations, and
     * multiply the raw form by some number K will result in a duration K times as long as the original. The value
     * 0 represents no time. In short, you can think of the unit as "Fooseconds", where Foo might be micro, nano,
     * or something else but similar.</p>
     *
     * <p>To get values of this TClass in a meaningful way, you should use one of the {@linkplain #secondsIntervalAs}
     * overloads, specifying the units you want. Units will truncate (not round) their values, as is standard in the
     * JDK's TimeUnit implementation.</p>
     *
     * <p>If you have a value in some format, and want to convert it to the SECONDS raw format, use
     * {@linkplain #secondsRawFrom(long, TimeUnit)} or {@linkplain #secondsRawFromFractionalSeconds(long)}.
     * The resulting value can be added to other raw SECONDS values intuitively, as explained above.</p>
     */
    public static final AkInterval SECONDS = new AkInterval(
            AkBundle.INSTANCE.id(),
            "interval seconds",
            AkCategory.DATE_TIME,
            SecondsAttrs.class,
            secondsFormatter,
            1,
            1,
            8,
            PUnderlying.INT_64,
            SecondsAttrs.FORMAT,
            AkIntervalSecondsFormat.values()
    );

    /**
     * Gets the interval from a source, which should correspond to an AkInterval.SECONDS value, in some unit.
     * @param source the source
     * @param as the desired unit
     * @return the source's value in the requested unit
     */
    public static long secondsIntervalAs(PValueSource source, TimeUnit as) {
        return secondsIntervalAs(source.getInt64(), as);
    }

    /**
     * Gets the interval from a raw long, which should correspond to an AkInterval.SECONDS value, in some unit
     * @param secondsIntervalRaw the raw form of the seconds value
     * @param as the desired unit
     * @return the raw value, translated to the requested unit
     */
    public static long secondsIntervalAs(long secondsIntervalRaw, TimeUnit as) {
        return as.convert(secondsIntervalRaw, AkIntervalSecondsFormat.UNDERLYING_UNIT);
    }

    /**
     * Gets a raw SECONDS value from an interval specified in some unit.
     * @param source the interval to translate to the raw form
     * @param sourceUnit the incoming interval's unit
     * @return the raw form
     */
    public static long secondsRawFrom(long source, TimeUnit sourceUnit) {
        return AkIntervalSecondsFormat.UNDERLYING_UNIT.convert(source, sourceUnit);
    }

    /**
     * <p>Gets the raw SECONDS value from a number that represents fractions of a second. For instance, 1 would
     * represent a tenth of a second; 123 would represent 123 milliseconds, etc. Values representing a greater
     * precision than the raw form supports will be truncated. The raw form won't be more precise than nanoseconds.</p>
     *
     * <p>Negative values are fine and are interpreted as if the negative sign were in front of the whole number.</p>
     *
     * <p>Examples:
     * <ul>
     *     <li>123 represents 123 milliseconds, and corresponds to 0.123 seconds.</li>
     *     <li>-4 represents 4 tenths of a second in the past, and corresponds to -0.4 seconds.</li>
     *     <li>123456789444 represents 123456789 nanoseconds, since the trailing 444 are past nanosecond resolution
     *     (and are thus sure to be truncated)</li>
     * </ul>
     * </p>
     * @param source the fractional component of time, as explained above
     * @return the raw form
     */
    public static long secondsRawFromFractionalSeconds(long source) {
        // We'll normalize this to nanoseconds, and then convert those nanos to the underlying unit. This may be
        // slightly inefficient, but it keeps a nice separation of concerns. The JDK's TimeUnit doesn't go further
        // than nanos, so we don't need to, either.
        int numberOfDigits = 0;
        for (long tmp = source; tmp != 0; tmp /= 10)
            ++numberOfDigits;

        final int GOAL = 9;
        int tooManyDigits = numberOfDigits - GOAL;

        if (tooManyDigits > 0) {
            // need to truncate
            while (tooManyDigits-- > 0)
                source /= 10;
        }

        if (tooManyDigits < 0) {
            // need to multiply, so that 1 becomes 100000000
            while (tooManyDigits++ < 0)
                source *= 10;
        }

        // source is now in nanos.
        return secondsRawFrom(source, TimeUnit.NANOSECONDS);
    }

    private enum SecondsAttrs implements Attribute {
        FORMAT
    }

    private enum MonthsAttrs implements Attribute {
        FORMAT
    }

    @Override
    public void attributeToString(int attributeIndex, long value, StringBuilder output) {
        if (attributeIndex == formatAttribute.ordinal())
            attributeToString(formatters, value, output);
        else
            super.attributeToString(attributeIndex, value,  output);
    }

    @Override
    protected TInstancePicker defaultPicker() {
        return picker;
    }

    @Override
    public TInstance instance() {
        return instance(formatAttribute.ordinal());
    }

    @Override
    protected void validate(TInstance instance) {
        int formatId = instance.attribute(formatAttribute);
        if ( (formatId < 0) || (formatId >= formatters.length) )
            throw new IllegalNameException("unrecognized literal format ID: " + formatId);
    }

    @Override
    public void putSafety(TExecutionContext context, TInstance sourceInstance, PValueSource sourceValue,
                          TInstance targetInstance, PValueTarget targetValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected DataTypeDescriptor dataTypeDescriptor(TInstance instance) {
        Boolean isNullable = instance.nullability(); // on separate line to make NPE easier to catch
        int literalFormatId = instance.attribute(formatAttribute);
        IntervalFormat format = formatters[literalFormatId];
        TypeId typeId = format.getTypeId();
        return new DataTypeDescriptor(typeId, isNullable);
    }

    @Override
    public TFactory factory() {
        throw new UnsupportedOperationException();
    }

    public TInstance tInstanceFrom(DataTypeDescriptor type) {
        TypeId typeId = type.getTypeId();
        IntervalFormat format = typeIdToFormat.get(typeId);
        if (format == null)
            throw new IllegalArgumentException("couldn't convert " + type + " to " + name());
        TInstance result = instance(format.ordinal());
        result.setNullable(type.isNullable());
        return result;
    }

    private <A extends Enum<A> & Attribute> AkInterval(TBundleID bundle, String name,
                                               Enum<?> category, Class<A> enumClass,
                                               TClassFormatter formatter,
                                               int internalRepVersion, int sVersion, int sSize,
                                               PUnderlying pUnderlying,
                                               A formatAttribute,
                                               IntervalFormat[] formatters)
    {
        super(bundle, name, category, enumClass, formatter, internalRepVersion, sVersion, sSize, pUnderlying,
                createParser(formatAttribute, formatters));
        this.formatters = formatters;
        this.formatAttribute = formatAttribute;
        this.typeIdToFormat = createTypeIdToFormatMap(formatters);

    }

    public boolean isDate()
    {
        return formatters[0] instanceof AkIntervalMonthsFormat
                || formatters[0] == AkIntervalSecondsFormat.DAY;
    }
    
    public boolean isTime()
    {
        return !isDate();
    }

    private final IntervalFormat[] formatters;
    private final Attribute formatAttribute;
    private final Map<TypeId,IntervalFormat> typeIdToFormat;
    private final TInstancePicker picker = new TInstancePicker() {
        @Override
        protected TInstance apply(TInstance left, TInstance right) {
            return instance();
        }
    };

    interface IntervalFormat {
        long parse(String string);
        TypeId getTypeId();
        int ordinal();
    }

    private static <F extends IntervalFormat> TParser createParser(final Attribute formatAttribute,
                                                                   final F[] formatters)
    {
        return new TParser() {
            @Override
            public void parse(TExecutionContext context, PValueSource in, PValueTarget out) {
                TInstance instance = context.outputTInstance();
                int literalFormatId = instance.attribute(formatAttribute);
                F format = formatters[literalFormatId];
                String inString = in.getString();
                long months = format.parse(inString);
                out.putInt64(months);
            }
        };
    }

    private static <F extends IntervalFormat> Map<TypeId, F> createTypeIdToFormatMap(F[] values) {
        Map<TypeId, F> map = new HashMap<TypeId, F>(values.length);
        for (F literalFormat : values)
            map.put(literalFormat.getTypeId(), literalFormat);
        return map;
    }

    static enum AkIntervalMonthsFormat implements IntervalFormat {
        YEAR("Y+", TypeId.INTERVAL_YEAR_ID),
        MONTH("M+", TypeId.INTERVAL_MONTH_ID),
        YEAR_MONTH("Y+-M?", TypeId.INTERVAL_YEAR_MONTH_ID)
        ;

        @Override
        public TypeId getTypeId() {
            return typeId;
        }

        @Override
        public long parse(String string) {
            return parser.parse(string);
        }

        AkIntervalMonthsFormat(String pattern, TypeId typeId) {
            this.parser = new MonthsParser(this, pattern);
            this.typeId = typeId;

        }

        private final AkIntervalParser<?> parser;
        private final TypeId typeId;

        private static class MonthsParser extends AkIntervalParser<Boolean> {

            private MonthsParser(Enum<?> onBehalfOf, String pattern) {
                super(onBehalfOf, pattern);
            }

            @Override
            protected boolean buildChar(char c, boolean checkBounds, ParseCompilation<? super Boolean> result) {
                switch (c) {
                case 'Y':
                    result.addUnit(Boolean.TRUE, -1, checkBounds);
                    break;
                case 'M':
                    result.addUnit(Boolean.FALSE, 12, checkBounds);
                    break;
                case '-':
                    return true;
                default:
                    throw new IllegalArgumentException("illegal pattern: " + result.inputPattern());
                }
                return false;
            }

            @Override
            protected long parseLong(long parsed, Boolean isYear) {
                if (isYear)
                    parsed = LongMath.checkedMultiply(parsed, 12);
                return parsed;
            }
        }
    }

    static enum AkIntervalSecondsFormat implements IntervalFormat {

        DAY("D+", TypeId.INTERVAL_DAY_ID),
        HOUR("H+", TypeId.INTERVAL_HOUR_ID),
        MINUTE("M+", TypeId.INTERVAL_MINUTE_ID),
        SECOND("S+u", TypeId.INTERVAL_SECOND_ID, true),
        DAY_HOUR("D+ H+", TypeId.INTERVAL_DAY_HOUR_ID),
        DAY_MINUTE("D+ H?:M?", TypeId.INTERVAL_DAY_MINUTE_ID),
        DAY_SECOND("D+ H?:M?:S?u", TypeId.INTERVAL_DAY_SECOND_ID),
        HOUR_MINUTE("H+:M?", TypeId.INTERVAL_HOUR_MINUTE_ID),
        HOUR_SECOND("H+:M?:S?u", TypeId.INTERVAL_HOUR_SECOND_ID),
        MINUTE_SECOND("M+:S?u", TypeId.INTERVAL_MINUTE_SECOND_ID)
        ;

        static TimeUnit UNDERLYING_UNIT = TimeUnit.MICROSECONDS;

        @Override
        public TypeId getTypeId() {
            return typeId;
        }

        @Override
        public long parse(String string) {
            return parser.parse(string);
        }

        AkIntervalSecondsFormat(String pattern, TypeId typeId) {
            this(pattern, typeId, false);
        }

        AkIntervalSecondsFormat(String pattern, TypeId typeId, boolean needsLeadingZero) {
            this.parser = new SecondsParser(this, pattern, needsLeadingZero);
            this.typeId = typeId;
        }

        private final AkIntervalParser<?> parser;
        private final TypeId typeId;

        private static class SecondsParser extends AkIntervalParser<TimeUnit> {

            private SecondsParser(Enum<?> onBehalfOf, String pattern, boolean needsLeadingZero) {
                super(onBehalfOf, pattern);
                this.needsLeadingZero = needsLeadingZero;
            }

            @Override
            protected String preParse(String string) {
                return (needsLeadingZero && (string.charAt(0) == '.'))
                        ? '0' + string
                        : string;
            }

            @Override
            protected boolean buildChar(char c, boolean checkBOunds, ParseCompilation<? super TimeUnit> result) {
                switch (c) {
                case 'D':
                    result.addUnit(TimeUnit.DAYS, 31, checkBOunds);
                    break;
                case 'H':
                    result.addUnit(TimeUnit.HOURS, 32, checkBOunds);
                    break;
                case 'M':
                    result.addUnit(TimeUnit.MINUTES, 59, checkBOunds);
                    break;
                case 'S':
                    result.addUnit(TimeUnit.SECONDS, 59, checkBOunds);
                    break;
                case 'u':
                    result.addUnit(null, -1, checkBOunds); // fractional component
                    break;
                case ' ':
                case ':':
                    return true;
                default:
                    throw new IllegalArgumentException("illegal pattern: " + result.inputPattern());
                }
                return false;
            }

            @Override
            protected String preParseSegment(String string, TimeUnit unit) {
                if (string == null)
                    return "0"; // inefficient because we'll just parse this, but oh well
                if ( (unit == null) && (string.length() > 8) )
                    string = string.substring(0, 9);
                return string;
            }

            @Override
            protected long parseLong(long parsedLong, TimeUnit parsedUnit) {
                if (parsedUnit != null) {
                    return UNDERLYING_UNIT.convert(parsedLong, parsedUnit);
                }
                else {
                    // Fractional seconds component. Need to be careful about how many digits were given.
                    // We'll normalize to nanoseconds, then convert to what we need. This isn't the most efficient,
                    // but it means we can change the underlying scale without having to remember this code.
                    // It's just a couple multiplications and one division, anyway.
                    return secondsRawFromFractionalSeconds(parsedLong);
                }
            }

            private final boolean needsLeadingZero;
        }
    }

    /**
     * A simple parser. The rules are:
     * <ul>
     *     <li>capital letters are special and correspond to numerical digits. If you have a capital letter followed
     *     by a '+', it means one or more digits, and the number's bounds shouldn't be checked. If it's followed by
     *     a '?', it means one or two digits, and the number's bounds should be checked. Otherwise, however many
     *     of the same character are in a row, that's how many digits are required (no more, no less). For instance,
     *     <tt>Y+ M? DD</tt> means any number of year digits (and the number can be as big as we want), followed by
     *     1 or 2 month digits (and the number's bounds will be checked), followed by exactly two days digits (and
     *     the number's bounds will be checked). The bounds come from #buildChar</li>
     *     <li></li>
     *     <li>a lowercase 'u' means a fractional component</li>
     *     <li>all other letters are non-special</li>
     * </ul>
     * @param <U>
     */
    static abstract class AkIntervalParser<U> {

        public long parse(String string) {
            boolean isNegative = (string.charAt(0) == '-');
            if (isNegative)
                string = string.substring(1);
            string = preParse(string);
            Matcher matcher = regex.matcher(string);
            if (!matcher.matches())
                throw new AkibanInternalException("couldn't parse string as " + onBehalfOf.name() + ": " + string);
            long result = 0;
            for (int i = 0, len = matcher.groupCount(); i < len; ++i) {
                String group = matcher.group(i+1);
                @SuppressWarnings("unchecked")
                U unit = (U) units[i];
                String preparsedGroup = preParseSegment(group, unit);
                Long longValue = Long.parseLong(preparsedGroup);
                int max = maxes[i];
                if (longValue > max)
                    throw new AkibanInternalException("out of range: " + group + " while parsing " + onBehalfOf);
                long parsed = parseLong(longValue, unit);
                result = LongMath.checkedAdd(result, parsed);
            }
            return isNegative ? -result : result;
        }

        protected abstract boolean buildChar(char c, boolean checkBounds, ParseCompilation<? super U> result);
        protected abstract long parseLong(long value, U unit);

        protected String preParse(String string) {
            return string;
        }

        protected String preParseSegment(String string, U unit) {
            return string;
        }

        protected AkIntervalParser(Enum<?> onBehalfOf, String pattern) {
            ParseCompilation<U> built = compile(pattern);
            this.regex = Pattern.compile(built.patternBuilder.toString());
            this.units = built.unitsList.toArray();
            this.onBehalfOf = onBehalfOf;
            int maxesSize = built.maxes.size();
            this.maxes = new int[maxesSize];
            for (int i = 0; i < maxesSize; ++i) {
                int max = built.maxes.get(i);
                this.maxes[i] = (max >= 0) ? max : Integer.MAX_VALUE;
            }
        }

        private final Enum<?> onBehalfOf;
        private final Pattern regex;
        private final Object[] units;
        private final int[] maxes;

        private static final int WILD_PLUS = -1;
        private static final int WILD_QUESTION = -2;

        private ParseCompilation<U> compile(String pattern) {
            ParseCompilation<U> result = new ParseCompilation<U>(pattern);
            for (int i = 0, len = pattern.length(); i < len; ++i) {
                boolean checkBounds = true;
                char c = pattern.charAt(i);
                if (c == 'u') {
                    result.patternBuilder.append("(?:\\.(\\d+))?");
                }
                else if (Character.isUpperCase(c)) {
                    int count;
                    int lookahead = i + 1;
                    if (lookahead == len) {
                        count = 1;
                    }
                    else if (pattern.charAt(lookahead) == '+') {
                        count = WILD_PLUS;
                    }
                    else if (pattern.charAt(lookahead) == '?') {
                        count = WILD_QUESTION;
                    }
                    else {
                        for(; lookahead < len; ++lookahead) {
                            if (pattern.charAt(lookahead) != c)
                                break;
                        }
                        count = lookahead - i;
                    }
                    switch (count) {
                    case WILD_PLUS:
                        result.patternBuilder.append("(\\d+)");
                        ++i;
                        checkBounds = false;
                        break;
                    case WILD_QUESTION:
                        result.patternBuilder.append("(\\d{1,2})");
                        ++i;
                        break;
                    default:
                        assert count > 0 : count;
                        result.patternBuilder.append("(\\d{").append(count).append("})");
                        i += (count-1);
                        break;
                    }
                }
                if (buildChar(c, checkBounds, result))
                    result.patternBuilder.append(c);
            }
            return result;
        }

        static class ParseCompilation<U> {

            public void addUnit(U unit, int max, boolean checkBounds) {
                unitsList.add(unit);
                maxes.add(checkBounds ? max : -1);
            }

            public String inputPattern() {
                return inputPattern;
            }

            ParseCompilation(String inputPattern) {
                this.inputPattern = inputPattern;
            }

            private String inputPattern;
            private StringBuilder patternBuilder = new StringBuilder();
            private List<U> unitsList = new ArrayList<U>();
            private List<Integer> maxes = new ArrayList<Integer>();
        }
    }
}
