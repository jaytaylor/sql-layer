/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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


        StringListConfigurationHandler stringsConfig = new StringListConfigurationHandler();
        InputStream testIS = YamlConfigurationTest.class.getResourceAsStream(yamlFileName);
        if (testIS == null) {
            throw new FileNotFoundException(yamlFileName);
        }
        int segment = 1;
        do {
            Reader testReader = new InputStreamReader(testIS, "UTF-8");
            YamlConfiguration yamlConfig = new YamlConfiguration(yamlFileName, testReader, null);
            try {
                yamlConfig.loadInto(stringsConfig);
            } finally {
                testReader.close();
            }
            String nextFile = yamlFileName + '.' + (segment++);
            testIS = YamlConfigurationTest.class.getResourceAsStream(nextFile);
        } while (testIS != null);

        assertEquals("output", Strings.join(expecteds), Strings.join(stringsConfig.strings()));
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
