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

package com.foundationdb.server.entity.changes;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.server.entity.model.Space;
import com.foundationdb.server.entity.model.diff.JsonDiffPreview;
import com.foundationdb.util.JUnitUtils;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;

import static com.foundationdb.util.JsonUtils.readTree;
import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class SpaceDiffTest {
    private static final String ORIG_SUFFIX = "-orig.json";
    private static final String UPDATE_SUFFIX = "-update.json";
    private static final String EXPECTED_SUFFIX = "-expected.json";
    private static final String UUIDS_SUFFIX = "-uuids.properties";

    private static String getShortName(String testName) {
        return testName.substring(0, testName.length() - ORIG_SUFFIX.length());
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() throws IOException {
        String[] testNames = JUnitUtils.getContainingFile(SpaceDiffTest.class).list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(ORIG_SUFFIX) &&
                        new File(dir, getShortName(name) + UPDATE_SUFFIX).exists();
            }
        });
        return Collections2.transform(Arrays.asList(testNames), new Function<String, Parameterization>() {
            @Override
            public Parameterization apply(String testName) {
                String shortName = getShortName(testName);
                return new Parameterization(shortName, true, shortName);
            }
        });
    }

    @Test
    public void test() throws IOException {
        Function<String, UUID> uuidGenerator;
        File uuidsFile = new File(dir, testName + UUIDS_SUFFIX);
        if (uuidsFile.exists()) {
            final Properties uuids = new Properties();
            uuids.load(new FileReader(uuidsFile));
            uuidGenerator = new Function<String, UUID>() {
                @Override
                public UUID apply(String input) {
                    String uuid = uuids.getProperty(input);
                    return UUID.fromString(uuid);
                }
            };
        }
        else {
            uuidGenerator = null;
        }
        Space orig = Space.readSpace(testName + ORIG_SUFFIX, SpaceDiffTest.class, null);
        Space updated = Space.readSpace(testName + UPDATE_SUFFIX, SpaceDiffTest.class, uuidGenerator);
        JsonNode expected = readTree(new File(dir, testName + EXPECTED_SUFFIX));
        StringWriter writer = new StringWriter();
        JsonDiffPreview diff = SpaceDiff.apply(orig, updated, new JsonDiffPreview(writer));
        diff.finish();
        JsonNode actual = readTree(new StringReader(writer.toString()));
        assertEquals("changes", expected, actual);
    }

    public SpaceDiffTest(String testName) {
        this.testName = testName;
    }

    private final String testName;
    private static final File dir = JUnitUtils.getContainingFile(SpaceDiffTest.class);
}
