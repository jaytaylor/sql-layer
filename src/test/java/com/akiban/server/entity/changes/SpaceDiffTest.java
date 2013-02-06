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

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.server.entity.model.Attribute;
import com.akiban.server.entity.model.Entity;
import com.akiban.server.entity.model.EntityIndex;
import com.akiban.server.entity.model.Space;
import com.akiban.server.entity.model.Validation;
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
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.akiban.util.AssertUtils.assertCollectionEquals;

@RunWith(NamedParameterizedRunner.class)
public final class SpaceDiffTest {
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() throws IOException {
        String[] testNames = JUnitUtils.getContainingFile(SpaceDiffTest.class).list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith("-orig.json");
            }
        });
        return Collections2.transform(Arrays.asList(testNames), new Function<String, Parameterization>() {
            @Override
            public Parameterization apply(String testName) {
                String shortName = testName.substring(0, testName.length() - "-orig.json".length());
                return new Parameterization(shortName, true, shortName);
            }
        });
    }

    @Test
    public void test() throws IOException {
        Space orig = Space.readSpace(testName + "-orig.json", SpaceDiffTest.class);
        Space updated = Space.readSpace(testName + "-update.json", SpaceDiffTest.class);
        List<String> expected = Strings.dumpFile(new File(dir, testName + "-expected.txt"));
        Collections.sort(expected);
        StringChangeLog log = new StringChangeLog();
        new SpaceDiff(orig, updated).apply(log);
        Collections.sort(log.getMessages());
        assertCollectionEquals("changes", expected, log.getMessages());
    }

    public SpaceDiffTest(String testName) {
        this.testName = testName;
    }

    private final String testName;
    private static final File dir = JUnitUtils.getContainingFile(SpaceDiffTest.class);

    private static class StringChangeLog extends JUnitUtils.MessageTaker implements SpaceModificationHandler {

        @Override
        public void addEntry(UUID entityUuid) {
            message("add entry", entityUuid);
        }

        @Override
        public void dropEntry(Entity dropped) {
            message("drop entry", dropped);
        }

        @Override
        public void renameEntry(UUID entityUuid, String oldName) {
            message("rename entry", entityUuid, oldName);
        }

        @Override
        public void addAttribute(UUID attributeUuid) {
            message("add attribute", attributeUuid);
        }

        @Override
        public void dropAttribute(Attribute dropped) {
            message("drop attribute", dropped);
        }

        @Override
        public void renameAttribute(UUID attributeUuid, String oldName) {
            message("rename attribute", attributeUuid, oldName);
        }

        @Override
        public void changeScalarType(UUID scalarUuid, Attribute afterChange) {
            message("change scalar type", scalarUuid, afterChange.getType());
        }

        @Override
        public void changeScalarValidations(UUID scalarUuid, Attribute afterChange) {
            message("change scalar validation", scalarUuid, afterChange.getValidation());
        }

        @Override
        public void changeScalarProperties(UUID scalarUuid, Attribute afterChange) {
            message("change scalar properties", scalarUuid, afterChange);
        }

        @Override
        public void addEntityValidation(Validation validation) {
            message("add entity validation", validation);
        }

        @Override
        public void dropEntityValidation(Validation validation) {
            message("drop entity validation", validation);
        }

        @Override
        public void addIndex(EntityIndex index) {
            message("add index", index);
        }

        @Override
        public void dropIndex(String name, EntityIndex index) {
            message("drop index", name, index);
        }

        @Override
        public void renameIndex(EntityIndex index, String oldName, String newName) {
            message("rename index", oldName, newName);
        }

        @Override
        public void error(String message) {
            message("error", message);
        }
    }
}
