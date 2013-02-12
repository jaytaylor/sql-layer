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

package com.akiban.server.entity.changes;

import com.akiban.ais.protobuf.ProtobufWriter;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.OnlyIf;
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
    private static final String TEST_NAME_SUFFIX = "-orig";
    private static final String INPUT_EXTENSION = ".json";
    private static final String EXPECTED_EXTENSION = ".ais";

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() throws IOException {
        final String fullSuffix = TEST_NAME_SUFFIX + INPUT_EXTENSION;
        String[] testNames = JUnitUtils.getContainingFile(EntityToAISTest.class).list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if(name.endsWith(fullSuffix)) {
                    String shortName = name.substring(0, name.length() - fullSuffix.length());
                    return new File(dir, shortName + TEST_NAME_SUFFIX + EXPECTED_EXTENSION).exists();
                }
                return false;
            }
        });
        return Collections2.transform(Arrays.asList(testNames), new Function<String, Parameterization>() {
            @Override
            public Parameterization apply(String testName) {
                String shortName = testName.substring(0, testName.length() - fullSuffix.length());
                return new Parameterization(shortName, true, shortName);
            }
        });
    }

    @Test
    public void test() throws IOException {
        Space spaceDef = Space.readSpace(testName + TEST_NAME_SUFFIX + ".json", EntityToAISTest.class);
        EntityToAIS eToAIS = new EntityToAIS(SCHEMA);
        spaceDef.visit(eToAIS);

        String expected = Strings.dumpFileToString(new File(dir, testName + TEST_NAME_SUFFIX + ".ais"));

        ProtobufWriter writer = new ProtobufWriter(new ProtobufWriter.SingleSchemaSelector(SCHEMA));
        String actual = writer.save(eToAIS.getAIS()).toString();

        assertEquals("Generated AIS", expected.trim(), actual.trim());
    }

    public EntityToAISTest(String testName) {
        this.testName = testName;
    }

    private final String testName;
    private static final File dir = JUnitUtils.getContainingFile(EntityToAISTest.class);
}
