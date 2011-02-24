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
import com.akiban.server.service.memcache.HapiProcessorFactory;
import com.akiban.server.service.memcache.ParsedHapiGetRequest;
import com.akiban.server.service.memcache.SimpleHapiPredicate;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
            return String.format("RowDataStruct(%s, %s)", rowData, Arrays.toString(backingBytes));
        }
    }

    private static class RowDataStructCollector implements HapiOutputter {
        private final List<RowDataStruct> rowDataStructs = new ArrayList<RowDataStruct>();
        @Override
        public void output(HapiProcessedGetRequest request, List<RowData> rows, OutputStream outputStream)
                throws IOException {
            for (RowData row : rows) {
                rowDataStructs.add(new RowDataStruct(row));
            }
        }

        public List<RowDataStruct> getRowDataStructs() {
            assertFalse("structs are empty!", rowDataStructs.isEmpty());
            return rowDataStructs;
        }
    }

    @Test
    public void sameRows() throws InvalidOperationException, HapiRequestException {
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

        final RowDataStructCollector first = new RowDataStructCollector();
        hapi(HapiProcessorFactory.CACHED).processRequest(session, request(), first, null);

        final RowDataStructCollector second = new RowDataStructCollector();
        hapi(HapiProcessorFactory.CACHED).processRequest(session, request(), second, null);

        assertEquals("row structs", first.getRowDataStructs(), second.getRowDataStructs());
        assertEquals("rows scanned", 3, first.getRowDataStructs().size());
    }

    private static HapiGetRequest request() throws HapiRequestException {
        // new but equivalent request each time
        return new HapiGetRequest() {
            @Override
            public String getSchema() {
                return "testSchema";
            }

            @Override
            public String getTable() {
                return "parent";
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
        };
    }
}
