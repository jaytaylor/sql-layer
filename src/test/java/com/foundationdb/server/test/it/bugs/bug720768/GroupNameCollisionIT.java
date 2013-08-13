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

package com.akiban.server.test.it.bugs.bug720768;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GroupNameCollisionIT extends ITBase {
    @Test
    public void tablesWithSameNames() {

        try {
            createTable("s1", "t", "id int not null primary key");
            createTable("s2", "t", "id int not null primary key");
            createTable("s1", "c", "id int not null primary key, pid int",
                    "GROUPING FOREIGN KEY (pid) REFERENCES t(id)");
            createTable("s2", "c", "id int not null primary key, pid int",
                    "GROUPING FOREIGN KEY (pid) REFERENCES t(id)");
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        AkibanInformationSchema ais = ddl().getAIS(session());
        final Group group1 = ais.getUserTable("s1", "t").getGroup();
        final Group group2 = ais.getUserTable("s2", "t").getGroup();
        if (group1.getName().equals(group2.getName())) {
            fail("same group names: " + group1 + " and " + group2);
        }

        UserTable s1T = ais.getUserTable("s1", "t");
        UserTable s1C = ais.getUserTable("s1", "c");
        UserTable s2T = ais.getUserTable("s2", "t");
        UserTable s2C = ais.getUserTable("s2", "c");

        assertEquals("s1.t root", s1T, group1.getRoot());
        assertEquals("s1.c parent", s1T, s1C.getParentJoin().getParent());
        assertEquals("s1.c join cols", "[JoinColumn(pid -> id)]", s1C.getParentJoin().getJoinColumns().toString());

        assertEquals("s2.t root", s2T, group2.getRoot());
        assertEquals("s2.c parent", s2T, s2C.getParentJoin().getParent());
        assertEquals("s2.c join cols", "[JoinColumn(pid -> id)]", s2C.getParentJoin().getJoinColumns().toString());
    }
}
