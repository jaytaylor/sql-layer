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

package com.akiban.sql.pg;

import com.akiban.sql.TestBase;

import com.akiban.server.rowdata.RowDef;

import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.RowOutput;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanFlag;
import com.akiban.sql.RegexFilenameFilter;

import org.junit.Ignore;

import java.io.File;
import java.util.EnumSet;

/**
 * A base class for integration tests that use data from files to specify the
 * input and output expected from calls to the Postgres server.
 */
@Ignore
public class PostgresServerFilesITBase extends PostgresServerITBase
{
    public void loadDatabase(File dir) throws Exception {
        rootTableId = loadSchemaFile(SCHEMA_NAME, new File(dir, "schema.ddl"));
        for (File data : dir.listFiles(new RegexFilenameFilter(".*\\.dat"))) {
            loadDataFile(SCHEMA_NAME, data);
        }
    }

    protected int rootTableId;

    protected String dumpData() throws Exception {
        final StringBuilder str = new StringBuilder();
        CursorId cursorId = dml()
            .openCursor(session(), aisGeneration(), 
                        new ScanAllRequest(rootTableId, null, 0,
                                           EnumSet.of(ScanFlag.DEEP)));
        dml().scanSome(session(), cursorId,
                       new RowOutput() {
                           public void output(NewRow row) {
                               RowDef rowDef = row.getRowDef();
                               str.append(rowDef.table().getName().getTableName());
                               for (int i = 0; i < rowDef.getFieldCount(); i++) {
                                   str.append(",");
                                   str.append(row.get(i));
                               }
                               str.append("\n");
                           }

                           public void mark() {
                           }
                           public void rewind() {
                           }
                       });
        dml().closeCursor(session(), cursorId);
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
