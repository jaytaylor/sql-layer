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

package com.akiban.server.test.it.bugs.bug1145249;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

public class ManyLongTextFieldsIT extends ITBase {
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";

    private void runTest(int fieldCount) {
        AISBuilder builder = new AISBuilder();
        builder.userTable(SCHEMA, TABLE);
        builder.column(SCHEMA, TABLE, "id", 0, "int", null, null, false, false, null, null);
        builder.index(SCHEMA, TABLE, Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(SCHEMA, TABLE, Index.PRIMARY_KEY_CONSTRAINT, "id", 0, true, null);
        for(int i = 1; i <= fieldCount; ++i) {
            String colName = "lt_" + i;
            builder.column(SCHEMA, TABLE, colName, i, "LONGTEXT", null, null, true, false, null, null);
        }
        UserTable table = builder.akibanInformationSchema().getUserTable(SCHEMA, TABLE);
        ddl().createTable(session(), table);
        updateAISGeneration();

        Object[] colValues = new Object[fieldCount + 1];
        colValues[0] = 1L;
        for(int i = 1; i <= fieldCount; ++i) {
            colValues[i] = "longtext_value";
        }

        int tid = tableId(SCHEMA, TABLE);
        NewRow row = createNewRow(tid, colValues);
        writeRows(row);
        expectFullRows(tid, row);
    }

    @Test
    public void oneField() {
        runTest(1);
    }

    @Test
    public void fiveFields() {
        runTest(5);
    }

    @Test
    public void tenFields() {
        runTest(10);
    }

    @Test
    public void twentyFields() {
        runTest(20);
    }

    @Test
    public void fiftyFields() {
        runTest(50);
    }
}
