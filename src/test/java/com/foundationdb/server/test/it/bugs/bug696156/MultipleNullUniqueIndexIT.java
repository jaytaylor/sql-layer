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

package com.foundationdb.server.test.it.bugs.bug696156;

import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MultipleNullUniqueIndexIT  extends ITBase {

    @Test
    public void reportTestCase() throws InvalidOperationException {
        String SCHEMA = "test";
        String TABLE = "t1";
        String COLUMN = "c1";
        TestAISBuilder builder = new TestAISBuilder(typesRegistry());
        builder.table(SCHEMA, TABLE);
        builder.column(SCHEMA, TABLE, COLUMN, 0, "MCOMPAT", "TINYINT", true, true);
        builder.unique(SCHEMA, TABLE, "c1");
        builder.indexColumn(SCHEMA, TABLE, COLUMN, COLUMN, 0, true, null);
        ddl().createTable(session(), builder.akibanInformationSchema().getTable(SCHEMA, TABLE));
        updateAISGeneration();
        final int tid = tableId(SCHEMA, TABLE);
        Object[] data = new Object[1];
        writeRows(row(tid, data));
        writeRows(row(tid, data));
        expectRowsSkipInternal(
            tid,
            row(tid, (Object)null),
            row(tid, (Object)null));
    }

    @Test
    public void singleColumnUniqueWithNulls() throws InvalidOperationException {
        final int tid = createTable("test", "t1", "id int not null primary key, name varchar(32), unique(name)");
        writeRows(row(tid, 1, "abc"),
                  row(tid, 2, "def"),
                  row(tid, 3, null),
                  row(tid, 4, "ghi"),
                  row(tid, 5, null));
        assertEquals(5, scanAll(tid).size());

        try {
            writeRows(row(tid, 6, "abc"));
            Assert.fail("DuplicateKeyException expected");
        }
        catch(DuplicateKeyException e) {
        }
    }

    @Test
    public void multiColumnUniqueWithNulls() throws InvalidOperationException {
        final int tid = createTable("test", "t1", "id int not null primary key, seg1 int, seg2 int, seg3 int, unique(seg1,seg2,seg3)");
        writeRows(row(tid, 1, 1, 1, 1),
                  row(tid, 2, 1, 1, null),
                  row(tid, 3, 1, null, 1),
                  row(tid, 4, 1, null, null),
                  row(tid, 5, null, 1, 1),
                  row(tid, 6, null, 1, null),
                  row(tid, 7, null, null, null),
                  row(tid, 8, null, null, null));
        assertEquals(8, scanAll(tid).size());

        try {
            writeRows(row(tid, 9, 1, 1, 1));
            Assert.fail("DuplicateKeyException expected");
        }
        catch(DuplicateKeyException e) {
        }
    }

    @Test
    public void singleColumnIndexWithNulls() throws InvalidOperationException {
        final int tid = createTable("test", "t1", "id int not null primary key, name varchar(32)");
        createIndex("test", "t1", "name", "name");
        writeRows(row(tid, 1, "abc"),
                  row(tid, 2, "def"),
                  row(tid, 3, "abc"),
                  row(tid, 4, null),
                  row(tid, 5, null));
        assertEquals(5, scanAll(tid).size());
    }

    @Test
    public void multiColumnIndexWithNulls() throws InvalidOperationException {
        final int tid = createTable("test", "t1", "id int not null primary key, seg1 int, seg2 int");
        createIndex("test", "t1", "seg1", "seg1", "seg2");
        writeRows(row(tid, 1, 1, 1),
                  row(tid, 2, 2, 2),
                  row(tid, 3, 1, 1),
                  row(tid, 4, 1, null),
                  row(tid, 5, null, 1),
                  row(tid, 6, 1, null),
                  row(tid, 7, null, 1),
                  row(tid, 8, null, null),
                  row(tid, 9, null, null));
        assertEquals(9, scanAll(tid).size());
    }
}
