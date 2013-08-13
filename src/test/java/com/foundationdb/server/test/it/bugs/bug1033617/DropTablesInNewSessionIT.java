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

package com.akiban.server.test.it.bugs.bug1033617;

import com.akiban.ais.model.TableName;
import com.akiban.server.service.session.Session;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

public final class DropTablesInNewSessionIT extends ITBase {
    @Test
    public void test() {
        int c = createTable("schema", "customers", "cid int not null primary key, name varchar(32)");
        int o = createTable("schema", "orders", "oid int not null primary key, cid int not null, placed date",
                akibanFK("cid", "customers", "cid"));
        TableName groupName = getUserTable(c).getGroup().getName();
        createGroupIndex(groupName, "name_placed", "customers.name,orders.placed");

        writeRow(c, 1L, "bob");
        writeRow(o, 11L, 1L, "2012-01-01");

        Collection<String> indexesToUpdate = Collections.singleton("name_placed");
        ddl().updateTableStatistics(session(), TableName.create("schema", "customers"), indexesToUpdate);

        Session session = serviceManager().getSessionService().createSession();
        try {
            dropAllTables(session);
        }
        finally {
            session.close();
        }
    }
}
