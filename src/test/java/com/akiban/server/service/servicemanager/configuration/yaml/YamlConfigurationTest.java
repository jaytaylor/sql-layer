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

package com.akiban.server.service.servicemanager.configuration.yaml;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.util.Strings;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class YamlConfigurationTest {

    @NamedParameterizedRunner.TestParameters
    public static List<Parameterization> parameterizations() throws Exception {
        List<String> fileNames = Strings.dumpResource(YamlConfigurationTest.class, ".");
        ParameterizationBuilder builder = new ParameterizationBuilder();
        for (String fileName : fileNames) {
            if (fileName.startsWith(TEST_PREFIX)) {
                if (fileName.endsWith(TEST_SUFFIX)) {
                    builder.add(fileName, fileName);
                } else if (fileName.endsWith(DISABLED_SUFFIX)) {
                    builder.addFailing(fileName, fileName);
                }
            }
        }
        return builder.asList();
    }

    @Test
    public void compare() throws Exception {
        assert yamlFileName.endsWith(TEST_SUFFIX) : yamlFileName;

        String expectedFileName = yamlFileName.substring(0, yamlFileName.length() - TEST_SUFFIX.length())
                + EXPECTED_SUFFIX;

        List<String> expecteds = Strings.dumpResource(YamlConfigurationTest.class, expectedFileName);


        StringListStrategy strategy = new StringListStrategy();
        YamlConfiguration configuration = new YamlConfiguration(strategy);
        InputStream testIS = YamlConfigurationTest.class.getResourceAsStream(yamlFileName);
        int segment = 1;
        do {
            Reader testReader = new InputStreamReader(testIS, "UTF-8");
            try {
                configuration.read(yamlFileName, testReader);
            } finally {
                testReader.close();
            }
            String nextFile = yamlFileName + '.' + (segment++);
            testIS = YamlConfigurationTest.class.getResourceAsStream(nextFile);
        } while (testIS != null);

        assertEquals("output", Strings.join(expecteds), Strings.join(strategy.strings()));
    }

    public YamlConfigurationTest(String yamlFileName) {
        this.yamlFileName = yamlFileName;
    }

    // object state

    private final String yamlFileName;

    // class state

    private final static String TEST_PREFIX = "test-";
    private final static String TEST_SUFFIX = ".yaml";
    private final static String DISABLED_SUFFIX = ".disabled";
    private final static String EXPECTED_SUFFIX = ".expected";
}
