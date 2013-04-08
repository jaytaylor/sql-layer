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

package com.akiban.server.entity.changes;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.NopVisitor;
import com.akiban.ais.model.UserTable;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.server.entity.model.Space;
import com.akiban.server.test.it.ITBase;
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

/**
 * Load an original space, load the diff, execute the DDLBasedSpaceModifier, and compare the resulting AIS.
 *
 * Input files: x-orig.json, x-update.json
 * Expected files: x-update.ais
 */
@RunWith(NamedParameterizedRunner.class)
public final class DDLBasedSpaceModifierIT extends ITBase {
    private static final String SCHEMA = "test";
    private static final String ORIG_SUFFIX = "-orig.json";
    private static final String UPDATE_SUFFIX = "-update.json";
    private static final String EXPECTED_SUFFIX = "-update.ais";

    private static String getShortName(String testName) {
        return testName.substring(0, testName.length() - ORIG_SUFFIX.length());
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() throws IOException {
        String[] testNames = JUnitUtils.getContainingFile(DDLBasedSpaceModifierIT.class).list(new FilenameFilter() {
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
        Space origSpace = Space.readSpace(testName + ORIG_SUFFIX, DDLBasedSpaceModifierIT.class, null);
        loadSpace(origSpace);
        Space updateSpace = Space.readSpace(testName + UPDATE_SUFFIX, DDLBasedSpaceModifierIT.class, null);

        SpaceDiff.apply(origSpace, updateSpace, new DDLBasedSpaceModifier(ddl(), session(), SCHEMA, updateSpace));

        String expected = Strings.dumpFileToString(new File(dir, testName + EXPECTED_SUFFIX));
        String actual = AISDumper.dumpDeterministicAIS(ais(), SCHEMA);
        assertEquals("Generated AIS", expected.trim(), actual.trim());
    }

    private void loadSpace(Space space) {
        EntityToAIS eToAIS = new EntityToAIS(SCHEMA);
        space.visit(eToAIS);
        AkibanInformationSchema ais = eToAIS.getAIS();
        for(Group group : ais.getGroups().values()) {
            UserTable root = group.getRoot();
            root.traverseTableAndDescendants(new NopVisitor() {
                @Override
                public void visitUserTable(UserTable table) {
                    ddl().createTable(session(), table);
                }
            });
            if(!group.getIndexes().isEmpty()) {
                ddl().createIndexes(session(), group.getIndexes());
            }
        }
    }

    public DDLBasedSpaceModifierIT(String testName) {
        this.testName = testName;
    }

    private final String testName;
    private static final File dir = JUnitUtils.getContainingFile(DDLBasedSpaceModifierIT.class);
}
