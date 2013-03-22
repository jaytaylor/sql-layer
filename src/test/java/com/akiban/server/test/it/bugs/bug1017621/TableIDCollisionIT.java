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

package com.akiban.server.test.it.bugs.bug1017621;

import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class TableIDCollisionIT extends ITBase {
    private static UserTable simpleISTable() {
        final TableName FAKE_TABLE = new TableName(TableName.INFORMATION_SCHEMA, "fake_table");
        NewAISBuilder builder = AISBBasedBuilder.create();
        builder.userTable(FAKE_TABLE).colLong("id").pk("id");
        UserTable table = builder.ais().getUserTable(FAKE_TABLE);
        assertNotNull("Found table", table);
        return table;
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        // Something unique, since we are messing with IS tables
        return uniqueStartupConfigProperties(getClass());
    }

    @Test
    public void createRestartAndCreate() throws Exception {
        createTable("test", "t1", "id int");
        safeRestartTestServices();
        createTable("test", "t2", "id int");
        serviceManager().getSchemaManager().registerStoredInformationSchemaTable(simpleISTable(), 1);
        createTable("test", "t3", "id int");
    }
}
