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

package com.akiban.server.store;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.akiban.server.CServerConstants;
import com.akiban.server.CServerTestCase;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;

public class SimpleBlobTest extends CServerTestCase implements CServerConstants {

    private final static String SIMPLE_BLOB_TEST_DDL = "simple_blob_test.ddl";


    @Before
    public void setUp() throws Exception {
        baseSetUp();
        setUpAisForTests(SIMPLE_BLOB_TEST_DDL);
    }

    @After
    public void tearDown() throws Exception {
        baseTearDown();
    }
    
    @Test
    public void testBlobs() throws Exception {
        final RowDef rowDef = rowDefCache.getRowDef("test.blobtest");
        final RowData rowData =new RowData(new byte[5000000]);
        final String[] expected = new String[7];
        for (int i = 1; i <= 6; i++) {
            int bsize = (int)Math.pow(5, i);
            int csize = (int)Math.pow(10, i);
            rowData.createRow(rowDef, new Object[]{i, bigString(bsize), bigString(csize)});
            expected[i] = rowData.toString(rowDefCache);
            store.writeRow(session, rowData);
        }
        
        final RowCollector rc = store.newRowCollector(session, rowDef.getRowDefId(), 0, 0, null, null, new byte[]{7});
        final ByteBuffer bb = ByteBuffer.allocate(5000000);
        for (int i = 1; i <= 6; i++) {
            assertTrue(rc.hasMore());
            bb.clear();
            assertTrue(rc.collectNextRow(bb));
            bb.flip();
            rowData.reset(bb.array(), 0, bb.limit());
            rowData.prepareRow(0);
            final String actual = rowData.toString(rowDefCache);
            assertEquals(expected[i], actual);
        }
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
