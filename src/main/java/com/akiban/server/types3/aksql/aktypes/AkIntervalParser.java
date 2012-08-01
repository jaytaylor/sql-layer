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
import com.google.common.math.LongMath;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

abstract class AkIntervalParser<U> {

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
