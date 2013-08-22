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

package com.foundationdb.server.test.it.store;

import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.Index;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SimpleBlobIT extends ITBase {
    private final String SCHEMA = "test";
    private final String TABLE = "blobtest";
    
    private int setUpTable() {
        AISBuilder builder = new AISBuilder();
        builder.userTable(SCHEMA, TABLE);
        builder.column(SCHEMA, TABLE, "a", 0, "int", null, null, false, false, null, null);
        builder.column(SCHEMA, TABLE, "b", 1, "blob", null, null, false, false, null, null);
        builder.column(SCHEMA, TABLE, "c", 2, "mediumblob", null, null, false, false, null, null);
        builder.index(SCHEMA, TABLE, Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn(SCHEMA, TABLE, Index.PRIMARY_KEY_CONSTRAINT, "a", 0, true, null);
        ddl().createTable(session(), builder.akibanInformationSchema().getUserTable(SCHEMA, TABLE));
        updateAISGeneration();
        return tableId(SCHEMA, TABLE);
    }
    
    @Test
    public void testBlobs_1() throws Exception {
        testBlobs(1);
    }

    @Test
    public void testBlobs_2() throws Exception {
        testBlobs(2);
    }

    @Test
    public void testBlobs_3() throws Exception {
        testBlobs(3);
    }

    @Test
    public void testBlobs_4() throws Exception {
        testBlobs(4);
    }

    @Test
    public void testBlobs_5() throws Exception {
        testBlobs(5);
    }

    @Test
    public void testBlobs_6() throws Exception {
        testBlobs(6);
    }

    private void testBlobs(int rowCount) throws Exception {
        final int tid = setUpTable();
        final List<NewRow> expected = new ArrayList<>();
        for (int i = 1; i <= rowCount; ++i) {
            int bsize = (int)Math.pow(5, i);
            int csize = (int)Math.pow(10, i);
            NewRow row = createNewRow(tid, (long)i, bigString(bsize), bigString(csize));
            writeRows(row);
            expected.add(row);
        }
        final List<NewRow> actual = scanAll(scanAllRequest(tid));
        assertEquals(expected, actual);
     }

    private String bigString(final int length) {
        final StringBuilder sb= new StringBuilder(length);
        sb.append(length);
        for (int i = sb.length() ; i < length; i++) {
            sb.append("#");
        }
        return sb.toString();
    }
}
