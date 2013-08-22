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

package com.foundationdb.server.test.it.bugs.bug1081621;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.util.TableChangeValidator;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

public class AlterAffectingPKLosesTreeIT extends ITBase {
    private final static String SCHEMA = "test";
    private final static TableName P_NAME = new TableName(SCHEMA, "p");
    private final static TableName C_NAME = new TableName(SCHEMA, "c");

    private void createTables() {
        createTable(P_NAME, "id int not null primary key, x int");
        createTable(C_NAME, "id int not null primary key, pid int, grouping foreign key(pid) references p(id)");
    }

    @Test
    public void test() throws Exception {
        createTables();

        runAlter(TableChangeValidator.ChangeLevel.GROUP, SCHEMA, "ALTER TABLE p DROP COLUMN id");
        runAlter(TableChangeValidator.ChangeLevel.GROUP, SCHEMA, "ALTER TABLE c DROP COLUMN id");

        ddl().dropTable(session(), P_NAME);
        ddl().dropTable(session(), C_NAME);

        safeRestartTestServices();

        createTables();
    }
}
