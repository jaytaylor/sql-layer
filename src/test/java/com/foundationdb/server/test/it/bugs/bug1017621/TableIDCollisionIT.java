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

package com.foundationdb.server.test.it.bugs.bug1017621;

import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class TableIDCollisionIT extends ITBase {
    private Table simpleISTable() {
        final TableName FAKE_TABLE = new TableName(TableName.INFORMATION_SCHEMA, "fake_table");
        NewAISBuilder builder = AISBBasedBuilder.create(ddl().getTypesTranslator());
        builder.table(FAKE_TABLE).colInt("id").pk("id");
        Table table = builder.ais().getTable(FAKE_TABLE);
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
