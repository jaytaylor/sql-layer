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

package com.foundationdb.server.test.it.bugs.bug720768;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.it.ITBase;
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
        final Group group1 = ais.getTable("s1", "t").getGroup();
        final Group group2 = ais.getTable("s2", "t").getGroup();
        if (group1.getName().equals(group2.getName())) {
            fail("same group names: " + group1 + " and " + group2);
        }

        Table s1T = ais.getTable("s1", "t");
        Table s1C = ais.getTable("s1", "c");
        Table s2T = ais.getTable("s2", "t");
        Table s2C = ais.getTable("s2", "c");

        assertEquals("s1.t root", s1T, group1.getRoot());
        assertEquals("s1.c parent", s1T, s1C.getParentJoin().getParent());
        assertEquals("s1.c join cols", "[JoinColumn(pid -> id)]", s1C.getParentJoin().getJoinColumns().toString());

        assertEquals("s2.t root", s2T, group2.getRoot());
        assertEquals("s2.c parent", s2T, s2C.getParentJoin().getParent());
        assertEquals("s2.c join cols", "[JoinColumn(pid -> id)]", s2C.getParentJoin().getJoinColumns().toString());
    }
}
