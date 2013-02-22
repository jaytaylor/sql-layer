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

package com.akiban.server.entity.fromais;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.NopVisitor;
import com.akiban.ais.model.UserTable;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.server.entity.model.Space;
import com.akiban.server.rowdata.SchemaFactory;
import com.akiban.util.JUnitUtils;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static com.akiban.util.JUnitUtils.normalizeJson;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(NamedParameterizedRunner.class)
public final class AisToSpaceTest {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() throws IOException {
        String[] testNames = testDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".json") && !name.endsWith("-uuids.json");
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
    public void test() {
        Space expectedSpace = Space.readSpace(testName + ".json", AisToSpaceTest.class);

        AkibanInformationSchema ais = SchemaFactory.loadAIS(new File(testDir, testName + ".ddl"), "test_schema");
        ais.traversePostOrder(new SetUuidAssigner());
        Space actualSpace = AisToSpace.create(ais);

        assertEquals("space json", normalizeJson(expectedSpace.toJson()), normalizeJson(actualSpace.toJson()));
    }

    private class SetUuidAssigner extends NopVisitor {

        @Override
        public void visitUserTable(UserTable userTable) {
            UUID uuid = setUuids.get(userTable.getName().getTableName());
            assertNotNull("uuid for " + userTable, uuid);
            userTable.setUuid(uuid);
        }

        @Override
        public void visitColumn(Column column) {
            UUID uuid = setUuids.get(column.getName());
            assertNotNull("uuid for " + column, uuid);
            column.setUuid(uuid);
        }

        private SetUuidAssigner() {
            String fileName = testName + "-uuids.json";
            Map<String, String> asStrings;
            try {
                ObjectMapper mapper = new ObjectMapper();
                Map asMap = mapper.readValue(new File(testDir, fileName), Map.class);
                asStrings = Collections.checkedMap(asMap, String.class, String.class);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            setUuids = Maps.transformValues(asStrings, new Function<String, UUID>() {
                @Override
                public UUID apply(String input) {
                    return UUID.fromString(input);
                }
            });
        }

        private final Map<String, UUID> setUuids;
    }

    public AisToSpaceTest(String testName) {
        this.testName = testName;
    }

    private final String testName;
    private static final File testDir = JUnitUtils.getContainingFile(AisToSpaceTest.class);
}
