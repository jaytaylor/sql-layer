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
    };

    /**
     * A MONTHS interval, whose 64-bit value represents number of months.
     */
    public static AkInterval MONTHS = new AkInterval(
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
    public static AkInterval SECONDS = new AkInterval(
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
        if (tooManyDigits > 0) { // need to truncate
            while (tooManyDigits-- > 0)
                source /= 0;
        }
        else if (tooManyDigits < 0) { // need to multiply, so that 1 becomes 100000000
            while (tooManyDigits++ > 0)
                source *= 0;
        }

        // source is now in nanos.
        return secondsRawFrom(source, TimeUnit.NANOSECONDS);
    }

    /**
     * Converts seconds, including fractional parts of a second, to the raw form. Precision past that of the underlying
     * representation will be truncated
     * @param source seconds
     * @return the raw form
     * @throws  IllegalArgumentException if the source is NaN or infinite, or if it represents more nanoseconds than
     * can be represented by a long
     */
    public static long secondsRawFromSeconds(double source) {
        if (Double.isNaN(source) || Double.isInfinite(source))
            throw new IllegalArgumentException("out of range: " + source);
        source *= 1000000000;
        if ( (source > ((double)Long.MAX_VALUE)) || (source < ((double)Long.MIN_VALUE)))
            throw new IllegalArgumentException("out of range: " + source);
        return secondsRawFrom((long)source, TimeUnit.NANOSECONDS);
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
    protected TInstance doPickInstance(TInstance instance0, TInstance instance1) {
        return instance();
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
    public DataTypeDescriptor dataTypeDescriptor(TInstance instance) {
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

    private final IntervalFormat[] formatters;
    private final Attribute formatAttribute;
    private final Map<TypeId,IntervalFormat> typeIdToFormat;

    private interface IntervalFormat {
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
        YEAR("Y", TypeId.INTERVAL_YEAR_ID),
        MONTH("M", TypeId.INTERVAL_MONTH_ID),
        YEAR_MONTH("Y-M", TypeId.INTERVAL_YEAR_MONTH_ID)
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
            protected void buildChar(char c, ParseCompilation<? super Boolean> result) {
                switch (c) {
                case 'Y':
                    result.addGroupingDigits();
                    result.addUnit(Boolean.TRUE);
                    break;
                case 'M':
                    result.addUnit(Boolean.FALSE);
                    result.addGroupingDigits();
                    break;
                case '-':
                    result.addPattern(c);
                    break;
                default:
                    throw new IllegalArgumentException("illegal pattern: " + result.inputPattern());
                }
            }

            @Override
            protected long parseLong(String asString, Boolean isYear) {
                long parsed = Long.parseLong(asString);
                if (isYear)
                    parsed = LongMath.checkedMultiply(parsed, 12);
                return parsed;
            }
        }
    }

    static enum AkIntervalSecondsFormat implements IntervalFormat {

        DAY("D", TypeId.INTERVAL_DAY_ID),
        HOUR("H", TypeId.INTERVAL_HOUR_ID),
        MINUTE("M", TypeId.INTERVAL_MINUTE_ID),
        SECOND("S", TypeId.INTERVAL_SECOND_ID),
        DAY_HOUR("D H", TypeId.INTERVAL_DAY_HOUR_ID),
        DAY_MINUTE("D H:M", TypeId.INTERVAL_DAY_MINUTE_ID),
        DAY_SECOND("D H:M:S", TypeId.INTERVAL_DAY_SECOND_ID),
        HOUR_MINUTE("H:M", TypeId.INTERVAL_HOUR_MINUTE_ID),
        HOUR_SECOND("H:M:S", TypeId.INTERVAL_HOUR_SECOND_ID),
        MINUTE_SECOND("M:S", TypeId.INTERVAL_MINUTE_SECOND_ID)
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
            this.parser = new SecondsParser(this, pattern);
            this.typeId = typeId;
        }

        private final AkIntervalParser<?> parser;
        private final TypeId typeId;

        private static class SecondsParser extends AkIntervalParser<TimeUnit> {
            private SecondsParser(Enum<?> onBehalfOf, String pattern) {
                super(onBehalfOf, pattern);
            }

            @Override
            protected void buildChar(char c, ParseCompilation<? super TimeUnit> result) {
                switch (c) {
                case 'D':
                    result.addGroupingDigits();
                    result.addUnit(TimeUnit.DAYS);
                    break;
                case 'H':
                    result.addGroupingDigits();
                    result.addUnit(TimeUnit.HOURS);
                    break;
                case 'M':
                    result.addGroupingDigits();
                    result.addUnit(TimeUnit.MINUTES);
                    break;
                case 'S':
                    result.addPattern("(\\d+)(?:\\.(\\d+))?");
                    result.addUnit(TimeUnit.SECONDS);
                    result.addUnit(null); // fractional component
                    break;
                case ' ':
                case ':':
                    result.addPattern(c);
                    break;
                default:
                    throw new IllegalArgumentException("illegal pattern: " + result.inputPattern());
                }
            }

            @Override
            protected long parseLong(String asString, TimeUnit parsedUnit) {
                long parsed;
                if (parsedUnit != null) {
                    parsed = Long.parseLong(asString);
                    return UNDERLYING_UNIT.convert(parsed, parsedUnit);
                }
                else {
                    // Fractional seconds component. Need to be careful about how many digits were given.
                    // We'll normalize to nanoseconds, then convert to what we need. This isn't the most efficient,
                    // but it means we can change the underlying scale without having to remember this code.
                    // It's just a couple multiplications and one division, anyway.
                    if (asString.length() > 8)
                        asString = asString.substring(0, 9);
                    parsed = Long.parseLong(asString);
                    return secondsRawFromFractionalSeconds(parsed);
                }
            }
        }
    }

    static abstract class AkIntervalParser<U> {

        public long parse(String string) {
            boolean isNegative;
            if (string.charAt(0) == '-') {
                isNegative = true;
                string = string.substring(1);
            }
            else {
                isNegative = false;
            }
            Matcher matcher = regex.matcher(string);
            if (!matcher.matches())
                throw new AkibanInternalException("couldn't parse string as " + onBehalfOf.name() + ": " + string);
            long result = 0;
            for (int i = 0, len = matcher.groupCount(); i < len; ++i) {
                String group = matcher.group(i+1);
                @SuppressWarnings("unchecked")
                U unit = (U) units;
                long parsed = parseLong(group, unit);
                result = LongMath.checkedAdd(result, parsed);
            }
            return isNegative ? -result : result;
        }

        protected abstract void buildChar(char c, ParseCompilation<? super U> result);
        protected abstract long parseLong(String asString, U unit);

        protected AkIntervalParser(Enum<?> onBehalfOf, String pattern) {
            ParseCompilation<U> built = compile(pattern);
            this.regex = Pattern.compile(built.patternBuilder.toString());
            this.units = built.unitsList.toArray();
            this.onBehalfOf = onBehalfOf;
        }

        private final Enum<?> onBehalfOf;
        private final Pattern regex;
        private final Object[] units;

        private ParseCompilation<U> compile(String pattern) {
            ParseCompilation<U> result = new ParseCompilation<U>(pattern);
            for (int i = 0, len = pattern.length(); i < len; ++i) {
                char c = pattern.charAt(i);
                buildChar(c, result);
            }
            return result;
        }

        static class ParseCompilation<U> {

            public void addUnit(U unit) {
                unitsList.add(unit);
            }

            public void addPattern(String pattern) {
                patternBuilder.append(pattern);
            }

            public void addPattern(char pattern) {
                patternBuilder.append(pattern);
            }

            public void addGroupingDigits() {
                addPattern("(\\d+)");
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
        }
    }
}
