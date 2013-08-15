/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.server.entity.model;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.OnlyIf;
import com.foundationdb.junit.OnlyIfNot;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.util.JUnitUtils;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(NamedParameterizedRunner.class)
public final class SpaceValidationTest {
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() throws IOException {
        String[] testNames = JUnitUtils.getContainingFile(SpaceValidationTest.class).list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".json");
            }
        });
        return Collections2.transform(Arrays.asList(testNames), new Function<String, Parameterization>() {
            @Override
            public Parameterization apply(String testName) {
                String label = testName.substring(0, testName.length() - ".json".length());
                return new Parameterization(label, true, testName);
            }
        });
    }

    @Test(expected = IllegalEntityDefinition.class)
    @OnlyIfNot("expectedValid()")
    public void invalid() throws IOException {
        Space.readSpace(testName, SpaceValidationTest.class, null);
    }

    @Test
    @OnlyIf("expectedValid()")
    public void valid() throws IOException {
        Space.readSpace(testName, SpaceValidationTest.class, null);
    }

    public boolean expectedValid() {
        return ! testName.startsWith("neg-");
    }

    public SpaceValidationTest(String testName) {
        this.testName = testName;
    }

    private final String testName;
}
