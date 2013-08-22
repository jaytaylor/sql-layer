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

package com.foundationdb.junit;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public final class OnlyIfUsageTest {
    @NamedParameterizedRunner.TestParameters
    public static List<Parameterization> params() {
        ParameterizationBuilder builder = new ParameterizationBuilder();
        builder.add("foo", "foo");
        builder.add("bar", "bar");
        return builder.asList();
    }

    private final String string;
    public final boolean stringIsFoo;

    public OnlyIfUsageTest(String string) {
        this.string = string;
        stringIsFoo = "foo".equals(string);
    }

    @Test @OnlyIf("isFoo()")
    public void equalsFoo() {
        assertEquals("string", "foo", string);
    }

    @Test @OnlyIf("stringIsFoo")
    public void equalsFooByField() {
        assertEquals("string", "foo", string);
    }

    @Test @OnlyIfNot("isFoo()")
    public void notEqualsFoo() {
        if ("foo".equals(string)) {
            fail("found a foo!");
        }
    }

    @Test @OnlyIf("hasThreeChars()") @OnlyIfNot("lastCharR()")
    public void threeCharNoTrailingR() {
        assertEquals("string length", 3, string.length());
        assertFalse("last char was r! <" + string + '>', string.charAt(2) == 'r');
    }

    @Test
    public void stringNotNull() {
        assertNotNull("string", string);
    }

    public boolean isFoo() {
        return "foo".equals(string);
    }

    public boolean hasThreeChars() {
        return string.length() == 3;
    }

    public boolean lastCharR() {
        // for simplicity, we'll assume string not null, string.length > 0
        return string.charAt( string.length() - 1) == 'r';
    }
}
