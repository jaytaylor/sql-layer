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

enum AkIntervalMonthsFormat implements IntervalFormat {
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
