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

import com.akiban.sql.types.TypeId;
import com.google.common.math.LongMath;

import java.util.concurrent.TimeUnit;

enum AkIntervalSecondsFormat implements IntervalFormat {

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
