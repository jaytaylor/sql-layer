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

package com.foundationdb.server.types;

import com.foundationdb.server.types.service.ReflectiveInstanceFinder;
import com.foundationdb.server.types.value.ValueSource;

import com.foundationdb.util.AkibanAppender;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;

import static com.foundationdb.server.types.value.ValueSources.valuefromObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class TypeFormattingTestBase
{
    public static Object[] tCase(TClass tClass, Object o, String str, String json, String literal) {
        return new Object[] { tClass, valuefromObject(o, tClass.instance(true)), str, json, literal };
    }

    public static Collection<Object[]> checkParams(TBundleID bundle, Collection<Object[]> params, TClass... ignore) throws Exception {
        ReflectiveInstanceFinder finder = new ReflectiveInstanceFinder();
        Collection<? extends TClass> allTypes = finder.find(TClass.class);
        for(TClass tClass : allTypes) {
            if(tClass.name().bundleId() == bundle) {
                boolean found = false;
                for(Object[] tc : params) {
                    if(tc[0] == tClass) {
                        found = true;
                        break;
                    }
                }
                if(!found && !Arrays.asList(ignore).contains(tClass)) {
                    fail("no case for " + tClass.name());
                }
            }
        }
        return params;
    }

    private final ValueSource valueSource;
    private final String str;
    private final String json;
    private final String literal;

    public TypeFormattingTestBase(TClass tClass, ValueSource valueSource, String str, String json, String literal) {
        this.valueSource = valueSource;
        this.str = str;
        this.json = json;
        this.literal = literal;
    }

    private void checkNull(TClass tClass) {
        TInstance inst = tClass.instance(true);
        ValueSource source  = valuefromObject(null, inst);
        check(source, "NULL", "null", "NULL");
    }

    private static void check(ValueSource source, String formatted, String formattedJSON, String formattedLiteral) {
        FormatOptions FORMAT_OPTS = new FormatOptions();
        FORMAT_OPTS.set(FormatOptions.BinaryFormatOption.HEX);
        FORMAT_OPTS.set(FormatOptions.JsonBinaryFormatOption.HEX);
        String typeName = source.getType().typeClass().name().toString();
        StringBuilder sb = new StringBuilder();
        AkibanAppender out = AkibanAppender.of(sb);
        sb.setLength(0);
        if (formatted != null) {
            source.getType().format(source, out);
            assertEquals(typeName + " str", formatted, sb.toString());
        }
        sb.setLength(0);
        source.getType().formatAsJson(source, out, FORMAT_OPTS);
        assertEquals(typeName + " json", formattedJSON, sb.toString());
        sb.setLength(0);
        source.getType().formatAsLiteral(source, out);
        assertEquals(typeName + " literal", formattedLiteral, sb.toString());
    }


    @Test
    public void test() {
        DateTimeZone orig = DateTimeZone.getDefault();
        DateTimeZone.setDefault(DateTimeZone.UTC);
        try {
            checkNull(valueSource.getType().typeClass());
            check(valueSource, str, json, literal);
        } finally {
            DateTimeZone.setDefault(orig);
        }
    }

    @Test
    public void testFrenchCanadianLocale() {
        Locale orig = Locale.getDefault();
        Locale.setDefault(Locale.CANADA_FRENCH);
        try {
            test();
        } finally {
            Locale.setDefault(orig);
        }
    }
}
