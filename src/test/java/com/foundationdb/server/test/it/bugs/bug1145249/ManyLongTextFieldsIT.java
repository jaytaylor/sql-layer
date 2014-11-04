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

package com.foundationdb.server.test.it.bugs.bug1145249;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

public class ManyLongTextFieldsIT extends ITBase {
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";

    private void runTest(int fieldCount) {
        TestAISBuilder builder = new TestAISBuilder(typesRegistry());
        builder.table(SCHEMA, TABLE);
        builder.column(SCHEMA, TABLE, "id", 0, "MCOMPAT", "int", false);
        builder.pk(SCHEMA, TABLE);
        builder.indexColumn(SCHEMA, TABLE, Index.PRIMARY, "id", 0, true, null);
        for(int i = 1; i <= fieldCount; ++i) {
            String colName = "lt_" + i;
            builder.column(SCHEMA, TABLE, colName, i, "MCOMPAT", "LONGTEXT", true);
        }
        Table table = builder.akibanInformationSchema().getTable(SCHEMA, TABLE);
        ddl().createTable(session(), table);

        Object[] colValues = new Object[fieldCount + 1];
        colValues[0] = 1;
        for(int i = 1; i <= fieldCount; ++i) {
            colValues[i] = "longtext_value";
        }

        int tid = tableId(SCHEMA, TABLE);
        Row row = row(tid, colValues);
        writeRow(row);
        expectRows(tid, row);
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
