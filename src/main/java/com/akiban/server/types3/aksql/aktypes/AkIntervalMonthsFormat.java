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
import com.akiban.sql.types.TypeId;
import com.google.common.math.LongMath;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    AkIntervalMonthsFormat(String pattern, TypeId typeId) {
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
            case 'M':
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
