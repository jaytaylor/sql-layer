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
import com.akiban.server.types3.TClass;
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

import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class AkIntervalMonths extends TClassBase {

    /**
     * An SECONDS interval. Its underlying INT_64 represents months.
     */
    public static TClass DAYS = new AkIntervalMonths();

    public static TInstance tInstanceFrom(DataTypeDescriptor type) {
        TypeId typeId = type.getTypeId();
        MonthFormat format = typeIdToLiteralFormat.get(typeId);
        if (format == null)
            throw new IllegalArgumentException("couldn't convert " + type + " to " + DAYS);
        TInstance result = DAYS.instance(format.ordinal());
        result.setNullable(type.isNullable());
        return result;
    }

    private enum MonthsAttrs implements Attribute {
        FORMAT
    }

    private enum MonthFormat {
        YEAR("Y", TypeId.INTERVAL_YEAR_ID),
        MONTH("M", TypeId.INTERVAL_MONTH_ID),
        YEAR_MONTH("Y-M", TypeId.INTERVAL_YEAR_MONTH_ID)
        ;

        public long parseToMonths(String string) {
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
                throw new AkibanInternalException("couldn't parse string as " + name() + ": " + string);
            long months = 0;
            for (int i = 0, len = matcher.groupCount(); i < len; ++i) {
                String group = matcher.group(i+1);
                long parsed = Long.parseLong(group);
                if (isYear[i])
                    parsed = LongMath.checkedMultiply(parsed, 12);
                months = LongMath.checkedAdd(months, parsed);
            }

            return isNegative ? -months : months;
        }

        private MonthFormat(String pattern, TypeId typeId) {
            StringBuilder compiled = new StringBuilder();
            boolean[] all = new boolean[pattern.length()]; // we'll trim it later
            int flags = 0;
            for (int i = 0, len = pattern.length(); i < len; ++i) {
                char c = pattern.charAt(i);
                switch (c) {
                case 'Y':
                    all[flags++] = true;
                    compiled.append("(\\d+)");
                    break;
                case 'N':
                    all[flags++] = false;
                    compiled.append("(\\d+)");
                    break;
                case '-':
                    compiled.append(c);
                    break;
                default:
                    throw new IllegalArgumentException("illegal pattern: " + pattern);
                }

            }

            this.regex = Pattern.compile(compiled.toString());
            this.isYear = new boolean[flags];
            System.arraycopy(all, 0, this.isYear, 0, flags);
            this.typeId = typeId;

        }

        private final Pattern regex;
        private final boolean[] isYear;
        private final TypeId typeId;
    }

    @Override
    public TFactory factory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DataTypeDescriptor dataTypeDescriptor(TInstance instance) {
        Boolean isNullable = instance.nullability(); // on separate line to make NPE easier to catch
        TypeId typeId =  formatFor(instance).typeId;
        return new DataTypeDescriptor(typeId, isNullable);
    }

    @Override
    public void putSafety(TExecutionContext context, TInstance sourceInstance, PValueSource sourceValue,
                          TInstance targetInstance, PValueTarget targetValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TInstance instance() {
        return instance(MonthFormat.MONTH.ordinal());
    }

    @Override
    protected TInstance doPickInstance(TInstance instance0, TInstance instance1) {
        return instance();
    }

    @Override
    protected void validate(TInstance instance) {
        int formatId = instance.attribute(MonthsAttrs.FORMAT);
        if ( (formatId < 0) || (formatId >= MonthFormat.values().length) )
            throw new IllegalNameException("unrecognized literal format ID: " + formatId);
    }

    private AkIntervalMonths() {
        super(
                AkBundle.INSTANCE.id(),
                "interval months",
                AkCategory.DATE_TIME,
                MonthsAttrs.class,
                formatter,
                1,
                1,
                8,
                PUnderlying.INT_64,
                parser);
    }

    private static MonthFormat formatFor(TInstance instance) {
        int literalFormatId = instance.attribute(MonthsAttrs.FORMAT);
        return MonthFormat.values()[literalFormatId];
    }

    private static TClassFormatter formatter = new TClassFormatter() {
        @Override
        public void format(TInstance instance, PValueSource source, AkibanAppender out) {
            long months = source.getInt64();

            long years = months / 12;
            months -= (years * 12);

            Formatter formatter = new Formatter(out.getAppendable());
            formatter.format("INTERVAL '%d-%d", years, months);
        }
    };
    private static TParser parser = new TParser() {
        @Override
        public void parse(TExecutionContext context, PValueSource in, PValueTarget out) {
            TInstance instance = context.outputTInstance();
            MonthFormat format = formatFor(instance);
            String inString = in.getString();
            long months = format.parseToMonths(inString);
            out.putInt64(months);
        }
    };

    private static final Map<TypeId,MonthFormat> typeIdToLiteralFormat = createTypeIdToLiteralFormatMap();

    private static Map<TypeId, MonthFormat> createTypeIdToLiteralFormatMap() {
        MonthFormat[] values = MonthFormat.values();
        Map<TypeId, MonthFormat> map = new HashMap<TypeId, MonthFormat>(values.length);
        for (MonthFormat literalFormat : values)
            map.put(literalFormat.typeId, literalFormat);
        return map;
    }
}
