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

package com.akiban.server.entity.model;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.util.JUnitUtils;
import com.akiban.util.Strings;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.codehaus.jackson.JsonNode;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static com.akiban.util.JsonUtils.readTree;
import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class SpaceToJsonTest {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() throws IOException {
        String[] testNames = testDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".json") && !name.startsWith("neg-");
            }
        });
        return Collections2.transform(Arrays.asList(testNames), new Function<String, Parameterization>() {
            @Override
            public Parameterization apply(String testName) {
                String shortName = testName.substring(0, testName.length() - ".json".length());
                return new Parameterization(shortName, true, shortName);
            }
        });
    }

    @Test
    public void test() throws IOException {
        Space space = Space.readSpace(testName + ".json", SpaceToJsonTest.class, null);
        String actual = space.toJson();
        String expected = Strings.dumpFileToString(new File(testDir, testName + ".json"));

        JsonNode actualNode = readTree(actual);
        JsonNode expectedNode = readTree(expected);

        assertEquals("space to json", expectedNode, actualNode);
    }

    public SpaceToJsonTest(String testName) {
        this.testName = testName;
    }

    private final String testName;
    private static final File testDir = JUnitUtils.getContainingFile(SpaceToJsonTest.class);
}
