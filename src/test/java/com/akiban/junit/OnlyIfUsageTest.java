/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.junit;

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

    public OnlyIfUsageTest(String string) {
        this.string = string;
    }

    @Test @OnlyIf("isFoo")
    public void equalsFoo() {
        assertEquals("string", "foo", string);
    }

    @Test
    public void stringNotNull() {
        assertNotNull("string", string);
    }

    public boolean isFoo() {
        return "foo".equals(string);
    }
}
