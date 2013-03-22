
package com.akiban.server.entity.model;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.OnlyIf;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.Parameterization;
import com.akiban.util.JUnitUtils;
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
