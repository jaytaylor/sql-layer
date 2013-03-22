
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

import static com.akiban.util.JsonUtils.normalizeJson;
import static com.akiban.util.JsonUtils.readValue;
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
        Space expectedSpace = Space.readSpace(testName + ".json", AisToSpaceTest.class, null);

        AkibanInformationSchema ais = SchemaFactory.loadAIS(new File(testDir, testName + ".ddl"), "test_schema");
        ais.traversePostOrder(new SetUuidAssigner());
        Space actualSpace = AisToSpace.create(ais, null);

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
                Map asMap = readValue(new File(testDir, fileName), Map.class);
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
