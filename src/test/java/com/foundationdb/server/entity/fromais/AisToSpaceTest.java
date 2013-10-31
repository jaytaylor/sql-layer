/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server.entity.fromais;

import com.foundationdb.ais.model.AbstractVisitor;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.server.entity.model.Space;
import com.foundationdb.server.rowdata.SchemaFactory;
import com.foundationdb.util.JUnitUtils;
import com.foundationdb.util.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;
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

import static com.foundationdb.util.JsonUtils.readValue;
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
    public void test() throws IOException {
        Space expectedSpace = Space.readSpace(testName + ".json", AisToSpaceTest.class, null);

        AkibanInformationSchema ais = SchemaFactory.loadAIS(new File(testDir, testName + ".ddl"), "test_schema");
        ais.visit(new SetUuidAssigner());
        Space actualSpace = AisToSpace.create(ais, null);

        JsonNode expectedNode = JsonUtils.readTree(expectedSpace.toJson());
        JsonNode actualNode = JsonUtils.readTree(actualSpace.toJson());
        assertEquals("space json", expectedNode, actualNode);
    }

    private class SetUuidAssigner extends AbstractVisitor
    {
        @Override
        public void visit(Table table) {
            UUID uuid = setUuids.get(table.getName().getTableName());
            assertNotNull("uuid for " + table, uuid);
            table.setUuid(uuid);
        }

        @Override
        public void visit(Column column) {
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
