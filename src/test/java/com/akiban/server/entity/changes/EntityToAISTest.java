
package com.akiban.server.entity.changes;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.server.entity.model.Space;
import com.akiban.util.JUnitUtils;
import com.akiban.util.Strings;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class EntityToAISTest {
    private static final String SCHEMA = "test";
    private static final String ORIG_SUFFIX = ".json";
    private static final String EXPECTED_SUFFIX = ".ais";

    private static String getShortName(String testName) {
        return testName.substring(0, testName.length() - ORIG_SUFFIX.length());
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() throws IOException {
        String[] testNames = JUnitUtils.getContainingFile(EntityToAISTest.class).list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(ORIG_SUFFIX) &&
                       new File(dir, getShortName(name) + EXPECTED_SUFFIX).exists();
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
        Space spaceDef = Space.readSpace(testName + ORIG_SUFFIX, EntityToAISTest.class, null);
        EntityToAIS eToAIS = new EntityToAIS(SCHEMA);
        spaceDef.visit(eToAIS);

        String expected = Strings.dumpFileToString(new File(dir, testName + EXPECTED_SUFFIX));
        String actual = AISDumper.dumpDeterministicAIS(eToAIS.getAIS(), SCHEMA);
        assertEquals("Generated AIS", expected.trim(), actual.trim());
    }

    public EntityToAISTest(String testName) {
        this.testName = testName;
    }

    private final String testName;
    private static final File dir = JUnitUtils.getContainingFile(EntityToAISTest.class);
}
