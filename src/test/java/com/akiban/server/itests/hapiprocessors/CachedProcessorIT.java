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

package com.akiban.server.itests.hapiprocessors;

import com.akiban.ais.model.TableName;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowData;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiPredicate;
import com.akiban.server.api.HapiProcessedGetRequest;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.service.memcache.SimpleHapiPredicate;
import com.akiban.server.service.memcache.hprocessor.CachedProcessor;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public final class CachedProcessorIT extends ApiTestBase {

    private static class RowDataStruct {
        private final RowData rowData;
        private final byte[] backingBytes;

        private RowDataStruct(RowData rowData) {
            this.rowData = rowData;
            this.backingBytes = rowData.getBytes();
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof RowDataStruct) {
                RowDataStruct rds = (RowDataStruct)other;
                return this.rowData == rds.rowData && this.backingBytes == rds.backingBytes;
            }
            return false;
        }

        @Override
        public int hashCode() {
            int result = rowData != null ? rowData.hashCode() : 0;
            result = 31 * result + (backingBytes != null ? Arrays.hashCode(backingBytes) : 0);
            return result;
        }

        @Override
        public String toString() {
            return String.format("RDS(%s 0x%x, 0x%x)", rowData, rowData.hashCode(), backingBytes.hashCode());
        }
    }

    private static class RowDataStructCollector implements HapiOutputter {
        private final List<List<RowDataStruct>> rowDataStructs = new ArrayList<List<RowDataStruct>>();
        @Override
        public void output(HapiProcessedGetRequest request, Iterable<RowData> rows, OutputStream outputStream)
                throws IOException {
            List<RowDataStruct> list = new ArrayList<RowDataStruct>();
            for (RowData row : rows) {
                list.add(new RowDataStruct(row));
            }
            rowDataStructs.add(list);
        }

        public List<RowDataStruct> getRowDataStructs(int index) {
            assertFalse("structs are empty!", rowDataStructs.isEmpty());
            return rowDataStructs.get(index);
        }
    }

    private static class CountingCachedProcessor extends CachedProcessor {
        private final AtomicInteger count = new AtomicInteger(0);

        @Override
        protected void requestWasProcessed() {
            count.incrementAndGet();
        }

        public int getCount() {
            return count.get();
        }
    }

    @Test
    public void sameRows() throws InvalidOperationException, HapiRequestException {
        CountingCachedProcessor processor = new CountingCachedProcessor();
        final RowDataStructCollector collector = new RowDataStructCollector();

        processor.processRequest(session, request(), collector, null);
        processor.processRequest(session, request(), collector, null);

        assertEquals("rows scanned", 3, collector.getRowDataStructs(0).size());
        assertEquals("row structs", collector.getRowDataStructs(0), collector.getRowDataStructs(1));
        assertEquals("scanrows invocations", 1, processor.getCount());
    }

    @Test
    public void differentRows() throws InvalidOperationException, HapiRequestException {
        CountingCachedProcessor processor = new CountingCachedProcessor();
        final RowDataStructCollector collector = new RowDataStructCollector();

        processor.processRequest(session, request(), collector, null);
        processor.processRequest(session, request("zebra"), collector, null);
        processor.processRequest(session, request(), collector, null);

        assertEquals("rows scanned", 3, collector.getRowDataStructs(0).size());
        assertFalse("row structs were equal", collector.getRowDataStructs(0).equals(collector.getRowDataStructs(1)));
        assertEquals("scanrows invocations", 3, processor.getCount());
    }

    @Before
    public void setUp() throws InvalidOperationException {
        final int parent = createTable("testSchema", "parent",
                "id int key");
        final int zebra = createTable("testSchema", "zebra",
                "id int key",
                "pid int",
                "CONSTRAINT __akiban_fk_zebra FOREIGN KEY __akiban_fk_zebra(pid) REFERENCES parent(id)");
        writeRows(
                createNewRow(parent, 1L),
                createNewRow(zebra, 1L, 1L),
                createNewRow(zebra, 2L, 1L),
                createNewRow(parent, 2L)
        );
    }

    private static HapiGetRequest request() throws HapiRequestException {
        return request("parent");
    }

    private static HapiGetRequest request(final String table) throws HapiRequestException {
        return new HapiGetRequest() {
            @Override
            public String getSchema() {
                return "testSchema";
            }

            @Override
            public String getTable() {
                return table;
            }

            @Override
            public TableName getUsingTable() {
                return TableName.create(getSchema(), getTable());
            }

            @Override
            public List<HapiPredicate> getPredicates() {
                List<HapiPredicate> single = new ArrayList<HapiPredicate>();
                single.add( new SimpleHapiPredicate(getUsingTable(), "id", HapiPredicate.Operator.EQ, "1"));
                return single;
            }

            @Override
            public boolean equals(Object obj) {
                return obj.getClass().equals(this.getClass()) && getTable().equals(((HapiGetRequest) obj).getTable());
            }

            @Override
            public int hashCode() {
                return getTable().hashCode();
            }
        };
    }
}
