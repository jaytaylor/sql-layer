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
     * In fact, it almost definitely is not number of seconds; instead, the value is in some private format.</p>
     *
     * <p>To get values of this TClass in a meaningful way, you should use one of the {@linkplain #secondsIntervalAs}
     * overloads, specifying the units you want. Units will truncate (not round) their values, as is standard in the
     * JDK's TimeUnit implementation.</p>
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

    public static long secondsIntervalAs(PValueSource source, TimeUnit as) {
        return secondsIntervalAs(source.getInt64(), as);
    }

    public static long secondsIntervalAs(long secondsIntervalRaw, TimeUnit as) {
        return as.convert(secondsIntervalRaw, AkIntervalSecondsFormat.UNDERLYING_UNIT);
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
                }
                else {
                    // Fractional seconds component. Need to be careful about how many digits were given.
                    // We'll normalize to nanoseconds, then convert to what we need. This isn't the most efficient,
                    // but it means we can change the underlying scale without having to remember this code.
                    // It's just a couple multiplications and one division, anyway.
                    if (asString.length() > 8)
                        asString = asString.substring(0, 9);
                    parsed = Long.parseLong(asString);
                    // how many digits short of the full 8 are we? e.g., "123" is 5 short. Need to multiply it
                    // by shortBy*10 to get to nanos
                    for (int shortBy= (8 - asString.length()); shortBy > 0; --shortBy)
                        parsed = LongMath.checkedMultiply(parsed, 10L);
                    parsedUnit = TimeUnit.NANOSECONDS;
                }
                return UNDERLYING_UNIT.convert(parsed, parsedUnit);
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
