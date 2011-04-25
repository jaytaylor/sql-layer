/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.test.it.store;

import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SimpleBlobIT extends ITBase {

    @Test
    public void testBlobs() throws Exception {
        final int tid = createTable("test", "blobtest", "a int", "b blob", "c mediumblob", "primary key(a)");
        final List<NewRow> expected = new ArrayList<NewRow>();
        for (int i = 1; i <= 6; ++i) {
            int bsize = (int)Math.pow(5, i);
            int csize = (int)Math.pow(10, i);
            NewRow row = createNewRow(tid, i, bigString(bsize), bigString(csize));
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
