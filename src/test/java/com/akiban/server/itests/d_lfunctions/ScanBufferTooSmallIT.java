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

package com.akiban.server.itests.d_lfunctions;

import com.akiban.ais.model.TableName;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.api.dml.scan.BufferFullException;
import com.akiban.server.api.dml.scan.RowDataOutput;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.service.memcache.SimpleHapiPredicate;
import com.akiban.server.service.memcache.hprocessor.Scanrows;
import com.akiban.server.service.memcache.outputter.DummyOutputter;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertTrue;

public final class ScanBufferTooSmallIT extends ApiTestBase {

    private int cid;
    private int oid;
    private int iid;

    @Before
    public void createTables() throws InvalidOperationException {
        cid = createTable("ts", "c",
                "cid int key");
        oid = createTable("ts", "o",
                "oid int key",
                "cid int",
                "CONSTRAINT __akiban_fk_c FOREIGN KEY __akiban_fk_c (cid) REFERENCES c(cid)");
        iid = createTable("ts", "i",
                "iid int key",
                "oid int",
                "CONSTRAINT __akiban_fk_o FOREIGN KEY __akiban_fk_o (oid) REFERENCES o(oid)");

        writeRows(
                createNewRow(cid, 1),
                createNewRow(oid, 1, 1),
                createNewRow(iid, 1, 1),
                createNewRow(iid, 2, 1)
        );
    }

    @Test(timeout=5000,expected=BufferFullException.class)
    public void viaScanFull() throws InvalidOperationException, BufferFullException {
        int coiId = ddl().getAIS(session).getTable("ts", "c").getGroup().getGroupTable().getTableId();
        ScanRequest request = new ScanAllRequest(coiId, new HashSet<Integer>(Arrays.asList(1, 2, 3, 4, 5, 6)));
        ByteBuffer buffer = ByteBuffer.allocate(10);
        buffer.mark();
        RowDataOutput.scanFull(session, dml(), buffer, request);
    }

    @Test(timeout=5000)
    public void viaHapi() throws HapiRequestException {
        final HapiGetRequest request = new HapiGetRequest() {
            @Override
            public String getSchema() {
                return "ts";
            }

            @Override
            public String getTable() {
                return "c";
            }

            @Override
            public TableName getUsingTable() {
                return new TableName("ts", "c");
            }

            @Override
            public List<HapiPredicate> getPredicates() {
                return Arrays.<HapiPredicate>asList(
                        new SimpleHapiPredicate(getUsingTable(), "cid", HapiPredicate.Operator.EQ, "1")
                );
            }
        };
        Scanrows scanrows = Scanrows.instance();
        scanrows.getMXBean().setBufferCapacity(10);
        scanrows.processRequest(session, request, DummyOutputter.instance(), new ByteArrayOutputStream(1));
        final int capacity = scanrows.getMXBean().getBufferCapacity();
        assertTrue("buffer capacity is " + capacity, capacity > 10);
    }
}
