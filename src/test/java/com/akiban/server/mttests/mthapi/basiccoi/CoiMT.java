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

package com.akiban.server.mttests.mthapi.basiccoi;

import com.akiban.ais.model.TableName;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.mttests.mthapi.base.HapiMTBase;
import com.akiban.server.mttests.mthapi.base.HapiSuccess;
import com.akiban.server.mttests.mthapi.base.WriteThread;
import com.akiban.server.service.memcache.ParsedHapiGetRequest;
import com.akiban.server.service.session.Session;
import com.akiban.util.Strings;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public final class CoiMT extends HapiMTBase {

    private abstract static class Writer implements WriteThread {
        private Integer customer;
        private Integer order;
        private Integer item;

        @Override
        public final void setupWrites(DDLFunctions ddl, DMLFunctions dml, Session session)
                throws InvalidOperationException
        {
            ddl.createTable(session, "s1", "create table c(id int key)");
            ddl.createTable(session, "s1", "create table o(id int key, cid int, "
                    +" CONSTRAINT __akiban_o FOREIGN KEY __akiban_o (cid) REFERENCES c (id)"
                    +" )");
            ddl.createTable(session, "s1", "create table i(id int key, oid int, "
                    +" CONSTRAINT __akiban_i FOREIGN KEY __akiban_o (oid) REFERENCES o (id)"
                    +" )");

            customer = ddl.getTableId(session, new TableName("s1", "c") );
            order = ddl.getTableId(session, new TableName("s1", "o") );
            item = ddl.getTableId(session, new TableName("s1", "i") );

            setupRows(dml);
        }

        protected abstract void setupRows(DMLFunctions dml) throws InvalidOperationException;

        protected final int customers() {
            return customer;
        }

        protected final int orders() {
            return order;
        }

        protected final int items() {
            return item;
        }
    }

    @Test
    public void allWritesFirst() throws HapiRequestException, JSONException, IOException {
        WriteThread writeThread = new Writer() {
            @Override
            protected void setupRows(DMLFunctions dml) throws InvalidOperationException {
                dml.writeRow( session, createNewRow(customers(), 1) );

                dml.writeRow( session, createNewRow(orders(), 1, 1) );
                dml.writeRow( session, createNewRow(orders(), 2, 1) );

                dml.writeRow( session, createNewRow(items(), 1, 1) );
            }

            @Override
            public void ongoingWrites(DDLFunctions ddl, DMLFunctions dml, Session session)
                    throws InvalidOperationException
            {
                // no ongoing writes
            }
        };

        final HapiGetRequest request = ParsedHapiGetRequest.parse("s1:c:id=1");
        JSONObject expectedJSON = new JSONObject(
                Strings.join(Strings.dumpResource(CoiMT.class, "allWritesFirst_expected.json"))
        );
        final String expectedResponse = expectedJSON.toString(4);

        HapiSuccess readThread = new HapiSuccess() {
            @Override
            protected void validateSuccessResponse(HapiGetRequest request, JSONObject result) throws JSONException {
                assertEquals(request.toString(), expectedResponse, result.toString(4));
            }

            @Override
            protected HapiGetRequest pullRequest() {
                return request;
            }

            @Override
            protected int spawnCount() {
                return 2500;
            }
        };

        runThreads(writeThread, readThread);
    }
}
