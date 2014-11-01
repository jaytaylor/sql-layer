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

package com.foundationdb.sql.pg;

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.sql.TestBase;

import org.junit.Ignore;

import java.io.File;

/**
 * A base class for integration tests that use data from files to specify the
 * input and output expected from calls to the Postgres server.
 */
@Ignore
public class PostgresServerFilesITBase extends PostgresServerITBase
{
    public void loadDatabase(File dir) throws Exception {
        this.rootTableId = super.loadDatabase(SCHEMA_NAME, dir);
    }

    protected int rootTableId;

    protected String dumpData() throws Exception {
        final StringBuilder str = new StringBuilder();
        for(Row row : scanAll(getTable(rootTableId).getGroup())) {
            str.append(row.rowType().table().getName().getTableName());
            for (int i = 0; i < row.rowType().nFields(); i++) {
                str.append(",");
                str.append(ValueSources.toObject(row.value(i)));
            }
            str.append("\n");
        }
        return str.toString();
    }

    protected String caseName, sql, expected, error;
    protected String[] params;

    /** Parameterized version. */
    protected PostgresServerFilesITBase(String caseName, String sql, 
					String expected, String error,
					String[] params) {
        this.caseName = caseName;
        this.sql = sql.trim();
        this.expected = expected;
        this.error = error;
        this.params = params;
    }

    protected PostgresServerFilesITBase() {
    }

    protected void generateAndCheckResult() throws Exception {
        TestBase.generateAndCheckResult((TestBase.GenerateAndCheckResult)this, 
                                        caseName, expected, error);
    }

}
