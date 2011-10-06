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

package com.akiban.server.service.functions;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class GlobularFunctionsClassFinderTest {
    @Test
    public void findClasses() {
        FunctionsClassFinder finder = new GlobularFunctionsClassFinder("testfunctionpath.txt");
        List<Class<?>> expected = new ArrayList<Class<?>>();
        expected.add(PathOneClass.class);
        expected.add(PathTwoClass.class);
        assertEquals(expected, finder.findClasses());
    }
}
