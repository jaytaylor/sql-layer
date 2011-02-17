package com.akiban.server.itests.bugs.bug720768;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.itests.ApiTestBase;
import org.junit.Test;

import static org.junit.Assert.fail;

public class GroupNameCollisionIT extends ApiTestBase {
    @Test
    public void tablesWithSameNames() {
        final int s1_t;
        final int s2_t;

        try {
            s1_t = createTable("s1", "t", "id int key");
            s2_t = createTable("s2", "t", "id int key");
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        AkibanInformationSchema ais = ddl().getAIS(session);
        final Group group1 = ais.getUserTable("s1", "t").getGroup();
        final Group group2 = ais.getUserTable("s2", "t").getGroup();
        if (group1.getName().equals(group2.getName())) {
            fail("same group names: " + group1 + " and " + group2);
        }
    }
}
